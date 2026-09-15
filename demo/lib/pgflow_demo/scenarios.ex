defmodule PgflowDemo.Scenarios do
  @moduledoc """
  Finite scenario catalogue for the demo test bed.
  """

  alias PgflowDemo.Flows.{
    ArticleFlow,
    DelayedFlow,
    JsonFlow,
    MapFlow,
    OnboardingFlow,
    ParallelFlow,
    QueueIdentityFlow,
    RecoveryFlow,
    RetryFlow,
    RootMapFlow,
    ScheduledFlow,
    TimeoutFlow
  }

  alias PgflowDemo.Flows.Exhaustion.{FailFlow, SkipCascadeFlow, SkipFlow}

  alias PgflowDemo.Flows.Policies.{
    IfMetFlow,
    IfNotFlow,
    WhenUnmetFailFlow,
    WhenUnmetSkipCascadeFlow,
    WhenUnmetSkipFlow
  }

  alias PgflowDemo.Jobs.RecordJob

  @max_map_count 10
  @max_payload_bytes 1024
  @max_retry_attempts 10
  @max_delay_seconds 60
  @override_allowlist []

  @type kind :: :executable | :walkthrough

  @type preset :: %{
          title: String.t(),
          input: map(),
          expected: map()
        }

  @type descriptor :: %{
          id: String.t(),
          title: String.t(),
          features: [String.t()],
          kind: kind(),
          module: module() | nil,
          flow_slug: String.t() | nil,
          presets: %{String.t() => preset()},
          walkthrough: String.t() | nil
        }

  @spec list() :: [descriptor()]
  def list, do: Enum.map(scenario_definitions(), &normalize/1)

  @doc """
  Fetches a scenario descriptor by string id.
  """
  @spec fetch(String.t()) :: {:ok, descriptor()} | {:error, :unknown_scenario}
  def fetch(id) when is_binary(id) do
    case Map.get(index(), id) do
      nil -> {:error, :unknown_scenario}
      definition -> {:ok, normalize(definition)}
    end
  end

  defp scenario_definitions do
    [
      %{
        id: "parallel",
        title: "Parallel DAG fan-in",
        features: ["sequential_parallel_dag"],
        kind: :executable,
        module: ParallelFlow,
        presets: %{
          "default_fan_in" =>
            preset("Two branches merge", %{"seed" => 2}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "article",
        title: "Article integration",
        features: ["sequential_parallel_dag", "integration"],
        kind: :walkthrough,
        module: ArticleFlow,
        flow_slug: "article_flow",
        presets: %{},
        walkthrough: "Configured LLM/network integration via FlowDemoLive"
      },
      %{
        id: "map",
        title: "Dependent map",
        features: ["dependent_map"],
        kind: :executable,
        module: MapFlow,
        presets: %{
          "three_items" =>
            preset("Three mapped items", %{"count" => 3}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "root_map",
        title: "Root map variants",
        features: ["root_map"],
        kind: :executable,
        module: RootMapFlow,
        presets: %{
          "normal_list" =>
            preset("Normal list", %{"items" => [1, 2, 3]}, %{"run_status" => "completed"}),
          "empty_list" => preset("Empty list", %{"items" => []}, %{"run_status" => "completed"}),
          "scalar_item" => preset("Scalar item", %{"items" => 7}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "onboarding",
        title: "Conditional onboarding",
        features: ["conditional_steps"],
        kind: :executable,
        module: OnboardingFlow,
        presets: %{
          "premium_ok" =>
            preset("Premium plan", %{"plan" => "premium", "fail_email" => false}, %{
              "run_status" => "completed"
            }),
          "free_skip_cascade" =>
            preset("Free plan skip cascade", %{"plan" => "free", "fail_email" => false}, %{
              "run_status" => "completed"
            }),
          "fail_soft_email" =>
            preset("Fail-soft email", %{"plan" => "premium", "fail_email" => true}, %{
              "run_status" => "completed"
            })
        }
      },
      policy_scenario("policy_if_met", "If met", IfMetFlow, %{"mode" => "active"}),
      policy_scenario("policy_if_not", "If not", IfNotFlow, %{"mode" => "inactive"}),
      policy_scenario("policy_when_unmet_skip", "When unmet skip", WhenUnmetSkipFlow, %{
        "enabled" => false
      }),
      policy_scenario(
        "policy_when_unmet_skip_cascade",
        "When unmet skip cascade",
        WhenUnmetSkipCascadeFlow,
        %{
          "enabled" => false
        }
      ),
      %{
        id: "policy_when_unmet_fail",
        title: "When unmet fail",
        features: ["conditional_policies"],
        kind: :executable,
        module: WhenUnmetFailFlow,
        presets: %{
          "default" =>
            preset("When unmet fail", %{"enabled" => false}, %{"run_status" => "failed"})
        }
      },
      %{
        id: "retry",
        title: "Retry and attempt context",
        features: ["retry_backoff"],
        kind: :executable,
        module: RetryFlow,
        presets: %{
          "succeed_on_third_attempt" =>
            preset("Succeed on third attempt", %{"succeed_on_attempt" => 3}, %{
              "run_status" => "completed",
              "attempts_count" => 3
            })
        }
      },
      exhaustion_scenario("exhaustion_fail", "Exhaustion fail", FailFlow, %{
        "run_status" => "failed"
      }),
      exhaustion_scenario("exhaustion_skip", "Exhaustion skip", SkipFlow, %{
        "run_status" => "completed"
      }),
      exhaustion_scenario(
        "exhaustion_skip_cascade",
        "Exhaustion skip cascade",
        SkipCascadeFlow,
        %{
          "run_status" => "completed"
        }
      ),
      %{
        id: "timeout",
        title: "Effective timeout",
        features: ["timeout"],
        kind: :executable,
        module: TimeoutFlow,
        presets: %{
          "fast_path" =>
            preset("Fast path", %{"should_timeout" => false}, %{"run_status" => "completed"}),
          "slow_path" =>
            preset("Handler times out", %{"should_timeout" => true}, %{"run_status" => "failed"})
        }
      },
      %{
        id: "delayed",
        title: "Start delay",
        features: ["start_delay"],
        kind: :executable,
        module: DelayedFlow,
        presets: %{
          "one_second_delay" =>
            preset("One second delay", %{"label" => "delayed"}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "json",
        title: "JSON values",
        features: ["json_values"],
        kind: :executable,
        module: JsonFlow,
        presets: %{
          "false_and_null" =>
            preset("False and null", %{"flag" => false, "empty" => nil}, %{
              "run_status" => "completed"
            }),
          "scalar_value" =>
            preset("Scalar value", %{"scalar" => 42, "empty" => nil}, %{
              "run_status" => "completed"
            }),
          "list_values" =>
            preset("List values", %{"list" => [1, "a", false]}, %{"run_status" => "completed"}),
          "object_values" =>
            preset("Object values", %{"object" => %{"nested" => true, "count" => 2}}, %{
              "run_status" => "completed"
            })
        }
      },
      %{
        id: "record_job",
        title: "Record job",
        features: ["single_step_jobs"],
        kind: :executable,
        module: RecordJob,
        presets: %{
          "immediate_echo" =>
            preset("Immediate echo", %{"payload" => %{"message" => "hello"}}, %{
              "run_status" => "completed"
            }),
          "delayed_enqueue" =>
            preset(
              "Delayed enqueue",
              %{"payload" => %{"message" => "later"}, "_enqueue" => %{"delay_seconds" => 1}},
              %{
                "run_status" => "completed"
              }
            )
        }
      },
      %{
        id: "scheduled",
        title: "Scheduled flow",
        features: ["cron_flows_jobs"],
        kind: :executable,
        module: ScheduledFlow,
        flow_slug: "scheduled_flow",
        presets: %{
          "manual_tick" =>
            preset("Manual tick", %{"source" => "manual"}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "recovery",
        title: "Recovery walkthrough",
        features: ["recovery_otp"],
        kind: :executable,
        module: RecoveryFlow,
        presets: %{
          "blocked_handler" => preset("Blocked handler", %{}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "queue_identity",
        title: "Queue identity",
        features: ["queue_identity"],
        kind: :executable,
        module: QueueIdentityFlow,
        flow_slug: "MixedCaseDemo",
        presets: %{
          "mixed_case" =>
            preset("Mixed case slug", %{"value" => 42}, %{"run_status" => "completed"})
        }
      },
      %{
        id: "observability",
        title: "Observability",
        features: ["observability"],
        kind: :walkthrough,
        module: nil,
        presets: %{},
        walkthrough: "PgFlow dashboard and run history"
      },
      %{
        id: "startup_compilation",
        title: "Startup compilation",
        features: ["startup_compilation"],
        kind: :walkthrough,
        module: nil,
        presets: %{},
        walkthrough: "FlowStarter compiles demo definitions at worker startup"
      },
      %{
        id: "multi_language",
        title: "Multi-language compatibility",
        features: ["multi_language"],
        kind: :walkthrough,
        module: nil,
        presets: %{},
        walkthrough: "Parent upstream interoperability report"
      },
      %{
        id: "external_waits",
        title: "External waits",
        features: ["external_waits"],
        kind: :walkthrough,
        module: nil,
        presets: %{},
        walkthrough: "Future work — not available in the demo UI"
      }
    ]
  end

  @doc """
  Validates a preset key and optional input overrides.
  """
  @spec validate_preset(String.t(), String.t(), map()) ::
          {:ok, preset()} | {:error, :unknown_scenario | :unknown_preset | :invalid_preset_bounds}
  def validate_preset(scenario_id, preset_key, overrides \\ %{}) do
    with {:ok, descriptor} <- fetch(scenario_id),
         {:ok, preset} <- fetch_preset(descriptor, preset_key),
         :ok <- validate_override_keys(preset.input, overrides),
         :ok <- validate_bounds(Map.merge(preset.input, overrides)) do
      {:ok, %{preset | input: Map.merge(preset.input, overrides)}}
    end
  end

  @spec flow_slug(descriptor()) :: String.t()
  def flow_slug(%{flow_slug: slug}) when is_binary(slug), do: slug

  def flow_slug(%{module: module}) when not is_nil(module),
    do: module.__pgflow_slug__() |> Atom.to_string()

  defp index, do: Map.new(scenario_definitions(), &{&1.id, &1})

  defp fetch_preset(%{presets: presets}, preset_key) do
    case Map.fetch(presets, preset_key) do
      :error -> {:error, :unknown_preset}
      {:ok, preset} -> {:ok, preset}
    end
  end

  defp normalize(definition) do
    flow_slug =
      definition[:flow_slug] ||
        (definition[:module] && Atom.to_string(definition.module.__pgflow_slug__()))

    %{
      id: definition.id,
      title: definition.title,
      features: definition.features,
      kind: definition.kind,
      module: Map.get(definition, :module),
      flow_slug: flow_slug,
      presets:
        Map.new(definition.presets, fn {key, preset} ->
          {key,
           %{
             preset
             | expected:
                 Map.merge(preset.expected, expected_details(definition, key, preset.input))
           }}
        end),
      walkthrough: Map.get(definition, :walkthrough)
    }
  end

  defp expected_details(definition, key, input) do
    statuses =
      definition.module.__pgflow_definition__().steps
      |> Map.new(fn step -> {Atom.to_string(step.slug), "completed"} end)
      |> Map.merge(status_overrides(definition.id, key))

    %{"step_statuses" => statuses}
    |> Map.merge(expected_outputs(definition.id, input))
  end

  defp status_overrides("onboarding", "free_skip_cascade"),
    do: %{"setup_premium" => "skipped", "activate_perk" => "skipped"}

  defp status_overrides("onboarding", "fail_soft_email"), do: %{"send_welcome" => "skipped"}
  defp status_overrides("policy_when_unmet_skip", _), do: %{"gated" => "skipped"}

  defp status_overrides("policy_when_unmet_skip_cascade", _),
    do: %{"gated" => "skipped", "downstream" => "skipped"}

  defp status_overrides("policy_when_unmet_fail", _), do: %{"gated" => "failed"}
  defp status_overrides("exhaustion_fail", _), do: %{"always_fails" => "failed"}
  defp status_overrides("exhaustion_skip", _), do: %{"fail_soft" => "skipped"}

  defp status_overrides("exhaustion_skip_cascade", _),
    do: %{"fail_soft" => "skipped", "downstream" => "skipped"}

  defp status_overrides("timeout", "slow_path"), do: %{"slow" => "failed"}
  defp status_overrides(_, _), do: %{}

  defp expected_outputs("parallel", input),
    do: %{"outputs" => %{"merge" => %{"sum" => input["seed"] * 5}}}

  defp expected_outputs("map", %{"count" => count}),
    do: %{"outputs" => %{"aggregate" => %{"total" => count * (count + 1)}}}

  defp expected_outputs("json", input) do
    %{
      "outputs" => %{
        "emit" => %{
          "flag" => input["flag"],
          "empty" => input["empty"],
          "scalar" => input["scalar"],
          "list" => Map.get(input, "list", []),
          "object" => Map.get(input, "object", %{})
        }
      }
    }
  end

  defp expected_outputs("queue_identity", input),
    do: %{"outputs" => %{"work" => %{"slug" => "MixedCaseDemo", "value" => input["value"]}}}

  defp expected_outputs("recovery", _), do: %{"outputs" => %{"wait" => %{"released" => true}}}
  defp expected_outputs(_, _), do: %{}

  defp preset(title, input, expected) do
    %{title: title, input: input, expected: expected}
  end

  defp policy_scenario(id, title, module, input) do
    %{
      id: id,
      title: title,
      features: ["conditional_policies"],
      kind: :executable,
      module: module,
      presets: %{
        "default" => preset(title, input, %{"run_status" => "completed"})
      }
    }
  end

  defp exhaustion_scenario(id, title, module, expected) do
    %{
      id: id,
      title: title,
      features: ["retry_exhaustion"],
      kind: :executable,
      module: module,
      presets: %{
        "default" => preset(title, %{}, expected)
      }
    }
  end

  defp validate_override_keys(preset_input, overrides) do
    allowed = preset_input |> Map.keys() |> Kernel.++(@override_allowlist) |> MapSet.new()
    unknown = Enum.reject(Map.keys(overrides), &MapSet.member?(allowed, &1))

    if unknown == [] do
      :ok
    else
      {:error, :invalid_preset_bounds}
    end
  end

  defp validate_bounds(input) do
    with :ok <- validate_count(input),
         :ok <- validate_retry(input),
         :ok <- validate_delay(input) do
      validate_payload(input)
    end
  end

  defp validate_count(%{"count" => count})
       when not is_integer(count) or count < 0 or count > @max_map_count,
       do: {:error, :invalid_preset_bounds}

  defp validate_count(%{"items" => items}) when is_list(items) and length(items) > @max_map_count,
    do: {:error, :invalid_preset_bounds}

  defp validate_count(_), do: :ok

  defp validate_retry(%{"succeed_on_attempt" => attempts})
       when not is_integer(attempts) or attempts < 1 or attempts > @max_retry_attempts,
       do: {:error, :invalid_preset_bounds}

  defp validate_retry(_), do: :ok

  defp validate_delay(%{"_enqueue" => %{"delay_seconds" => delay}})
       when is_integer(delay) and delay > @max_delay_seconds,
       do: {:error, :invalid_preset_bounds}

  defp validate_delay(_), do: :ok

  defp validate_payload(input) do
    size = input |> Jason.encode!() |> byte_size()

    if size > @max_payload_bytes do
      {:error, :invalid_preset_bounds}
    else
      :ok
    end
  rescue
    _ -> {:error, :invalid_preset_bounds}
  end
end
