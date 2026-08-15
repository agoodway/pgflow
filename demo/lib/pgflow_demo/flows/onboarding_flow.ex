defmodule PgflowDemo.Flows.OnboardingFlow do
  @moduledoc """
  Demo flow that shows conditional skip, skip-cascade, and fail-soft email.

  DAG Structure:
  ```
  create_account
    ├─ setup_premium   if: %{create_account: %{plan: "premium"}}, when_unmet: :skip_cascade
    │    └─ activate_perk
    ├─ send_welcome    max_attempts: 1, when_exhausted: :skip
    └─ finish          depends_on: [:create_account]
  ```

  Dependent-step `if` matches `{dep_slug => output}`, not flow input, so
  `plan` and `fail_email` are copied through `create_account` output.

  Compiles to: `pgmq.onboarding_flow` queue and `pgflow.flows`/`pgflow.steps` rows in Postgres.
  """

  use PgFlow.Flow

  @flow queue: :onboarding_flow, max_attempts: 3, base_delay: 1, timeout: 30

  # Create the account and pass plan/fail_email to dependents
  step :create_account do
    fn input, _ctx ->
      %{
        "user_id" => input["user_id"] || "demo-user",
        "plan" => input["plan"],
        "fail_email" => input["fail_email"]
      }
    end
  end

  # Premium setup only when create_account output has plan: "premium"
  step :setup_premium,
    depends_on: [:create_account],
    if: %{create_account: %{plan: "premium"}},
    when_unmet: :skip_cascade do
    fn deps, _ctx ->
      %{"perks" => ["priority_support"], "account" => deps["create_account"]}
    end
  end

  # Cascades when setup_premium is skipped
  step :activate_perk, depends_on: [:setup_premium] do
    fn deps, _ctx ->
      %{"activated" => true, "perks" => deps["setup_premium"]["perks"]}
    end
  end

  # Fail-soft: one attempt, then skip so the run can still complete
  step :send_welcome,
    depends_on: [:create_account],
    max_attempts: 1,
    when_exhausted: :skip do
    fn deps, _ctx ->
      if deps["create_account"]["fail_email"] in [true, "true"] do
        raise "smtp timeout"
      end

      %{"sent" => true, "to" => deps["create_account"]["user_id"]}
    end
  end

  # Always runs after the account exists
  step :finish, depends_on: [:create_account] do
    fn deps, _ctx ->
      %{"ok" => true, "user_id" => deps["create_account"]["user_id"]}
    end
  end
end
