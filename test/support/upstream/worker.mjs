#!/usr/bin/env node
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

function upstreamImport(specifier) {
  const checkout = process.env.PGFLOW_UPSTREAM_CHECKOUT;
  const map = {
    postgres: 'node_modules/postgres/src/index.js',
    '@pgflow/dsl': 'pkgs/dsl/dist/index.js',
    '@pgflow/core': 'pkgs/core/dist/index.js',
    '@pgflow/edge-worker': 'pkgs/edge-worker/dist/index.js',
    '@pgflow/edge-worker/dist/core/supabase-utils.js':
      'pkgs/edge-worker/dist/core/supabase-utils.js',
  };
  const rel = map[specifier];
  if (!checkout || !rel) return specifier;
  return new URL(rel, `file://${checkout}/`).href;
}

const require = createRequire(upstreamImport('@pgflow/core'));
const postgres = require('postgres');
const { Flow, extractFlowShape } = await import(upstreamImport('@pgflow/dsl'));
const { PgflowSqlClient } = await import(upstreamImport('@pgflow/core'));
const { createFlowWorker } = await import(upstreamImport('@pgflow/edge-worker'));
const { createServiceSupabaseClient } = await import(
  upstreamImport('@pgflow/edge-worker/dist/core/supabase-utils.js')
);

const databaseUrl = process.env.DATABASE_URL ?? process.env.PGFLOW_COMPAT_DATABASE_URL;
const scenario = process.argv.includes('--scenario')
  ? process.argv[process.argv.indexOf('--scenario') + 1]
  : 'all';

if (!databaseUrl) {
  console.error('DATABASE_URL is required');
  process.exit(2);
}

const testSupabaseEnv = {
  SUPABASE_DB_URL: databaseUrl,
  SUPABASE_URL: 'https://interop.test',
  SUPABASE_ANON_KEY: 'test-anon',
  SUPABASE_SERVICE_ROLE_KEY: 'test-service',
  SB_EXECUTION_ID: 'interop-test',
};

function createTestPlatformAdapter(sql) {
  const abortController = new AbortController();
  return {
    get env() {
      return testSupabaseEnv;
    },
    get shutdownSignal() {
      return abortController.signal;
    },
    get platformResources() {
      return { sql, supabase: createServiceSupabaseClient(testSupabaseEnv) };
    },
    get connectionString() {
      return databaseUrl;
    },
    get isLocalEnvironment() {
      return false;
    },
    requestShutdown() {
      abortController.abort();
    },
    async startWorker() {},
    async stopWorker() {},
  };
}

function consoleLogger() {
  const noop = () => {};
  return {
    debug: noop,
    verbose: noop,
    info: noop,
    warn: noop,
    error: noop,
    taskStarted: (ctx) => {
      console.error(`claimed msg_id=${ctx.msgId}`);
    },
    taskCompleted: noop,
    taskFailed: noop,
    polling: noop,
    taskCount: noop,
    startupBanner: noop,
    shutdown: noop,
  };
}

async function ensureWorker(sql, queueName, workerId) {
  await sql`
    SELECT pgflow_tests.ensure_worker(
      ${queueName},
      ${workerId}::uuid,
      ${'interop_worker'}
    )
  `;
}

async function withSql(fn) {
  const sql = postgres(databaseUrl, { max: 4, prepare: false });
  try {
    return await fn(sql);
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function waitForRun(sql, runId, expectedStatus, timeoutMs = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const [row] = await sql`
      SELECT status FROM pgflow.runs WHERE run_id = ${runId}::uuid
    `;
    if (row?.status === expectedStatus) return row;
    await delay(100);
  }
  throw new Error(`run ${runId} did not reach ${expectedStatus}`);
}

function mixedFlow() {
  // Match Elixir flow defaults so both compilers persist the same execution options.
  const options = { maxAttempts: 1, baseDelay: 1, timeout: 30 };
  return new Flow({ slug: 'mixed_lang_flow', ...options })
    .step({ slug: 'prepare', ...options }, async () => {
      await delay(50);
      return { ok: true };
    })
    .step({ slug: 'process', dependsOn: ['prepare'], ...options }, async () => {
      await delay(50);
      return { ok: true };
    })
    .step({ slug: 'deliver', dependsOn: ['process'], ...options }, async () => {
      await delay(50);
      return { ok: true };
    });
}

async function installFlow(sql, flow) {
  const shape = extractFlowShape(flow);
  const [row] = await sql`
    SELECT pgflow.ensure_flow_compiled(
      ${flow.slug},
      ${sql.json(shape)}::jsonb,
      'flow',
      ${null}::jsonb
    ) AS result
  `;
  return row.result;
}

async function runFlowWorker(sql, flow, options = {}) {
  const worker = createFlowWorker(
    flow,
    {
      sql,
      maxConcurrent: 1,
      batchSize: options.batchSize ?? 5,
      visibilityTimeout: 30,
    },
    () => consoleLogger(),
    createTestPlatformAdapter(sql)
  );

  await worker.startOnlyOnce({
    edgeFunctionName: `worker_${flow.slug}`,
    workerId: randomUUID(),
  });

  return worker;
}

async function scenarioCompileMixedFlow() {
  return withSql(async (sql) => {
    const flow = mixedFlow();
    const result = await installFlow(sql, flow);
    return { status: result.status, slug: flow.slug };
  });
}

async function scenarioMixedWorker() {
  return withSql(async (sql) => {
    const flow = mixedFlow();
    const expected = Number(process.env.PGFLOW_MIXED_RUN_COUNT || '20');
    const worker = await runFlowWorker(sql, flow, { batchSize: 1 });
    const started = Date.now();

    try {
      while (Date.now() - started < 60_000) {
        const [row] = await sql`
          SELECT count(*)::int AS n
          FROM pgflow.runs
          WHERE flow_slug = ${flow.slug} AND status = 'completed'
        `;
        if (row.n >= expected) return { completed: row.n, slug: flow.slug };
        await delay(100);
      }
      throw new Error(`mixed worker timed out before ${expected} runs completed`);
    } finally {
      await worker.stop();
    }
  });
}

async function scenarioWorkerExistingFlow() {
  return withSql(async (sql) => {
    const slug = process.env.PGFLOW_INTEROP_FLOW_SLUG;
    if (!slug) throw new Error('PGFLOW_INTEROP_FLOW_SLUG is required');

    const flow = new Flow({ slug }).step({ slug: 'work' }, async (input) => ({
      producer: input.producer,
      value: (input.value ?? 0) + 1,
    }));

    const worker = await runFlowWorker(sql, flow);
    const runId = process.env.PGFLOW_INTEROP_RUN_ID;
    if (!runId) throw new Error('PGFLOW_INTEROP_RUN_ID is required');

    await waitForRun(sql, runId, 'completed');
    await worker.stop();
    return { run_id: runId };
  });
}

async function scenarioWorkerCompletesFlow() {
  return withSql(async (sql) => {
    const slug = `ts_worker_${randomUUID().slice(0, 8)}`;
    const flow = new Flow({ slug }).step({ slug: 'work' }, async (input) => ({
      producer: input.producer,
      value: (input.value ?? 0) + 1,
    }));

    await installFlow(sql, flow);
    const client = new PgflowSqlClient(sql);
    const run = await client.startFlow(slug, { producer: 'typescript', value: 3 });
    const worker = await runFlowWorker(sql, flow);
    await waitForRun(sql, run.run_id, 'completed');
    await worker.stop();
    return { run_id: run.run_id };
  });
}

async function scenarioStartFlowOnly() {
  return withSql(async (sql) => {
    const slug = process.env.PGFLOW_INTEROP_FLOW_SLUG;
    if (!slug) throw new Error('PGFLOW_INTEROP_FLOW_SLUG is required');

    const client = new PgflowSqlClient(sql);
    const run = await client.startFlow(slug, { producer: 'typescript', value: 4 });
    return { run_id: run.run_id, flow_slug: slug };
  });
}

async function scenarioBigintClaim() {
  return withSql(async (sql) => {
    const slug = `bigint_${randomUUID().slice(0, 8)}`;
    const flow = new Flow({ slug }).step({ slug: 'one' }, async () => ({ ok: true }));
    await installFlow(sql, flow);
    const client = new PgflowSqlClient(sql);
    const run = await client.startFlow(slug, {});
    const workerId = randomUUID();
    const queueName = slug.toLowerCase();
    const msgId = '9007199254740993';

    if (msgId !== String(BigInt(msgId))) {
      throw new Error('message ID lost precision in JavaScript');
    }

    const [task] = await sql`
      SELECT message_id::text AS message_id
      FROM pgflow.step_tasks
      WHERE run_id = ${run.run_id}::uuid AND step_slug = 'one'
    `;

    await ensureWorker(sql, queueName, workerId);

    const claimed = await client.startTasks(
      slug,
      [task.message_id],
      workerId,
      queueName
    );

    if (claimed.some((row) => row.msg_id !== task.message_id)) {
      throw new Error('message ID lost precision across startTasks');
    }

    const precisionRows = await sql`
      SELECT * FROM pgflow.start_tasks(
        ${slug},
        ${[msgId]}::bigint[],
        ${workerId}::uuid,
        ${queueName}
      )
    `;

    return {
      msg_id: claimed[0]?.msg_id,
      precision_probe: precisionRows.length === 0 ? 'no_matching_message' : 'claimed',
    };
  });
}

async function scenarioJsonFalsyProbe() {
  return withSql(async (sql) => {
    const probes = [];
    for (const value of [false, 0, '']) {
    const slug = `json_${randomUUID().slice(0, 8)}`;
    const flow = new Flow({ slug }).step({ slug: 'emit' }, async () => value);
    await installFlow(sql, flow);
    const client = new PgflowSqlClient(sql);
    const run = await client.startFlow(slug, {});
    const workerId = randomUUID();
    const queueName = slug.toLowerCase();
    const [message] = await client.readMessages(queueName, 30, 1, 1, 50);
    await ensureWorker(sql, queueName, workerId);
    await client.startTasks(slug, [String(message.msg_id)], workerId, queueName);
    await client.completeTask(
      { run_id: run.run_id, step_slug: 'emit', task_index: 0 },
      value
    );

    const [task] = await sql`
      SELECT output FROM pgflow.step_tasks
      WHERE run_id = ${run.run_id}::uuid AND step_slug = 'emit'
    `;

    if (task.output !== null && task.output !== value) {
      throw new Error(`Unexpected falsy output: ${JSON.stringify(task.output)}`);
    }
    probes.push({
      input: value,
      expected: value,
      actual: task.output,
      upstream_coerces_to_null: task.output === null,
      unresolved: task.output === null,
    });
    }
    return { ...probes[0], probes, unresolved: probes.some((probe) => probe.unresolved) };
  });
}

async function scenarioConcurrentWorkers() {
  return withSql(async (sql) => {
    const slug = `concurrent_${randomUUID().slice(0, 8)}`;
    const flow = new Flow({ slug }).step({ slug: 'count' }, async (input) => ({
      count: (input.count ?? 0) + 1,
    }));

    await installFlow(sql, flow);
    const client = new PgflowSqlClient(sql);
    const run = await client.startFlow(slug, { count: 0 });
    const workerA = await runFlowWorker(sql, flow);
    const workerB = await runFlowWorker(sql, flow);
    await waitForRun(sql, run.run_id, 'completed');
    await workerA.stop();
    await workerB.stop();

    const [task] = await sql`
      SELECT output FROM pgflow.step_tasks
      WHERE run_id = ${run.run_id}::uuid AND step_slug = 'count'
    `;

    return { output: task.output };
  });
}

const scenarios = {
  compile_mixed_flow: scenarioCompileMixedFlow,
  mixed_worker: scenarioMixedWorker,
  worker_existing_flow: scenarioWorkerExistingFlow,
  worker_completes_flow: scenarioWorkerCompletesFlow,
  ts_start: scenarioStartFlowOnly,
  bigint_claim: scenarioBigintClaim,
  json_falsy_probe: scenarioJsonFalsyProbe,
  concurrent_workers: scenarioConcurrentWorkers,
};

async function main() {
  const defaultScenarios = [
    'worker_completes_flow',
    'bigint_claim',
    'json_falsy_probe',
    'concurrent_workers',
  ];
  const selected = scenario === 'all' ? defaultScenarios : [scenario];
  const results = [];
  let failures = 0;
  let unresolved = 0;

  for (const name of selected) {
    const fn = scenarios[name];
    if (!fn) {
      failures += 1;
      results.push({ name, status: 'failed', error: 'unknown scenario' });
      continue;
    }

    try {
      const details = await fn();
      if (details.unresolved) unresolved += 1;
      results.push({ name, status: details.unresolved ? 'unresolved' : 'passed', details });
    } catch (error) {
      failures += 1;
      results.push({
        name,
        status: 'failed',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  process.stdout.write(
    JSON.stringify({
      status: failures === 0 ? 'passed' : 'failed',
      tests: selected.length - unresolved,
      failures,
      unresolved,
      results,
    })
  );

  if (failures > 0) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
