#!/usr/bin/env node
import { readFileSync } from 'node:fs';

function upstreamImport(specifier) {
  const checkout = process.env.PGFLOW_UPSTREAM_CHECKOUT;
  const map = {
    '@pgflow/dsl': 'pkgs/dsl/dist/index.js',
  };
  const rel = map[specifier];
  if (!checkout || !rel) return specifier;
  return new URL(rel, `file://${checkout}/`).href;
}

const { Flow, extractFlowShape } = await import(upstreamImport('@pgflow/dsl'));

const casesPath = process.env.PGFLOW_SHAPE_CASES_PATH;
const cases = JSON.parse(readFileSync(casesPath, 'utf8')).cases;

function flowOptions(definition) {
  const opts = { slug: definition.slug };
  for (const { key, value } of definition.opts ?? []) {
    opts[camelCase(key)] = value;
  }
  return opts;
}

function stepOptions(step) {
  const opts = {
    slug: step.slug,
    dependsOn: step.depends_on ?? [],
  };

  if (step.if !== undefined) opts.if = step.if;
  if (step.if_not !== undefined) opts.ifNot = step.if_not;
  if (step.when_unmet) opts.whenUnmet = step.when_unmet.replace('_', '-');
  if (step.when_exhausted) opts.whenExhausted = step.when_exhausted;
  if (step.max_attempts != null) opts.maxAttempts = step.max_attempts;
  if (step.base_delay != null) opts.baseDelay = step.base_delay;
  if (step.timeout != null) opts.timeout = step.timeout;
  if (step.start_delay != null) opts.startDelay = step.start_delay;

  return opts;
}

function depsDeclared(definition) {
  const slugs = new Set((definition.steps ?? []).map((step) => step.slug));

  return (definition.steps ?? []).every((step) =>
    (step.depends_on ?? []).every((dep) => slugs.has(dep))
  );
}

function tsConstructible(definition) {
  if (!depsDeclared(definition)) return false;

  const flowZeroDelay = (definition.opts ?? []).some(
    ({ key, value }) => key === 'base_delay' && value === 0
  );

  const stepZeroDelay = (definition.steps ?? []).some(
    (step) => step.base_delay === 0 || step.start_delay === 0
  );

  return !flowZeroDelay && !stepZeroDelay;
}

function buildFlow(definition) {
  let flow = new Flow(flowOptions(definition));

  for (const step of definition.steps ?? []) {
    const opts = stepOptions(step);
    const handler = () => ({ ok: true });

    if (step.step_type === 'map') {
      const mapOpts = { ...opts };
      if ((step.depends_on ?? []).length > 0) {
        mapOpts.array = step.depends_on[0];
        delete mapOpts.dependsOn;
      }

      flow = flow.map(mapOpts, handler);
    } else {
      flow = flow.step(opts, handler);
    }
  }

  return flow;
}

function camelCase(key) {
  return key.replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
}

const failures = [];
const skippedCases = [];
const allowedSkips = {
  charge_after_approval: 'Golden dependency refers to an undeclared approval step.',
  zero_delays: 'Pinned TypeScript DSL rejects zero delays accepted by SQL and Elixir.',
};

for (const testCase of cases) {
  if (!tsConstructible(testCase.definition)) {
    const reason = allowedSkips[testCase.id];
    if (!reason) failures.push({ id: testCase.id, error: 'Unapproved skipped shape case' });
    skippedCases.push({ id: testCase.id, reason: reason ?? 'Not allow-listed' });
    continue;
  }

  const actual = extractFlowShape(buildFlow(testCase.definition));
  if (JSON.stringify(actual) !== JSON.stringify(testCase.shape)) {
    failures.push({ id: testCase.id, expected: testCase.shape, actual });
  }
}

const testedCases = cases.filter((testCase) => tsConstructible(testCase.definition));

const report = {
  status: failures.length === 0 ? 'passed' : 'failed',
  tests: testedCases.length,
  skipped: cases.length - testedCases.length,
  skipped_cases: skippedCases,
  failures: failures.length,
  details: failures,
};

process.stdout.write(JSON.stringify(report));
if (failures.length > 0) process.exit(1);
