# LiveView Integration

`PgFlow.LiveClient` is a LiveView-native client for tracking flow and job runs in real-time. It manages PubSub subscriptions, loads run snapshots from the database, and applies incremental updates to `%PgFlow.Schema.Run{}` structs stored in your socket assigns — no manual PubSub wiring required.

## Prerequisites

Add the `:pubsub` option to your PgFlow config:

```elixir
# config/config.exs
config :my_app, MyApp.PgFlow,
  repo: MyApp.Repo,
  pubsub: MyApp.PubSub,
  flows: [MyApp.Flows.ProcessOrder]
```

This tells PgFlow's supervisor to attach telemetry handlers that broadcast run and task events to your PubSub.

## Quick Example

```elixir
defmodule MyAppWeb.OrderLive do
  use MyAppWeb, :live_view

  alias PgFlow.LiveClient

  def mount(_params, _session, socket) do
    {:ok, LiveClient.init(socket, pubsub: MyApp.PubSub)}
  end

  def handle_event("process", %{"order_id" => order_id}, socket) do
    case LiveClient.start_flow(socket, MyApp.Flows.ProcessOrder, %{"order_id" => order_id}) do
      {:ok, socket} -> {:noreply, socket}
      {:error, _reason, socket} -> {:noreply, socket}
    end
  end

  def handle_info({:pgflow, _, _} = msg, socket) do
    {:noreply, LiveClient.handle_info(msg, socket)}
  end

  def render(assigns) do
    ~H"""
    <button phx-click="process" phx-value-order_id="123">Process Order</button>

    <div :if={@flow_run}>
      <p>Status: {@flow_run.status}</p>

      <div :for={step <- @flow_run.step_states}>
        <p>{step.step_slug}: {step.status}</p>
      </div>
    </div>
    """
  end
end
```

## API

### `init/2`

Initializes the socket for flow tracking. Call this in `mount/3`.

```elixir
LiveClient.init(socket, pubsub: MyApp.PubSub)
```

Sets the default assign (`:flow_run`) to `nil` and stores PubSub config in socket private. Safe to call during both connected and disconnected mount.

**Options:**

| Option    | Default      | Description                  |
|-----------|--------------|------------------------------|
| `:pubsub` | (required)   | Your Phoenix.PubSub module   |
| `:as`     | `:flow_run`  | Assign key for the run       |

### `start_flow/4`

Starts a flow, subscribes to its PubSub topic, loads the initial snapshot, and assigns the `%Run{}`.

```elixir
case LiveClient.start_flow(socket, :process_order, %{"order_id" => 123}) do
  {:ok, socket} -> {:noreply, socket}
  {:error, reason, socket} -> {:noreply, socket}
end
```

Accepts a flow module, slug atom, or string slug as the second argument.

**Options:**

| Option | Default     | Description            |
|--------|-------------|------------------------|
| `:as`  | `:flow_run` | Assign key for the run |

### `enqueue/4`

Starts a job and subscribes to updates. Naming parity with `PgFlow.enqueue/2`. Behaves identically to `start_flow/4`.

```elixir
case LiveClient.enqueue(socket, MyApp.Jobs.SendEmail, %{"to" => "user@example.com"}) do
  {:ok, socket} -> {:noreply, socket}
  {:error, reason, socket} -> {:noreply, socket}
end
```

### `subscribe/3`

Subscribes to an existing run by ID. Useful for run detail pages where the run was started elsewhere.

```elixir
socket = LiveClient.subscribe(socket, run_id)
```

**Options:**

| Option | Default     | Description            |
|--------|-------------|------------------------|
| `:as`  | `:flow_run` | Assign key for the run |

### `unsubscribe/2`

Unsubscribes from a run and resets the assign to `nil`.

```elixir
socket = LiveClient.unsubscribe(socket)
socket = LiveClient.unsubscribe(socket, as: :my_run)
```

### `handle_info/2`

Applies a PubSub message to the tracked run in assigns. Wire this up in your LiveView:

```elixir
def handle_info({:pgflow, _, _} = msg, socket) do
  {:noreply, LiveClient.handle_info(msg, socket)}
end
```

Returns the socket unchanged if the message is for an untracked run.

## Tracking Multiple Runs

Use the `:as` option to track multiple runs simultaneously under different assign keys:

```elixir
def mount(_params, _session, socket) do
  socket =
    socket
    |> LiveClient.init(pubsub: MyApp.PubSub, as: :order_run)
    |> LiveClient.init(pubsub: MyApp.PubSub, as: :email_run)

  {:ok, socket}
end

def handle_event("start_order", params, socket) do
  {:ok, socket} = LiveClient.start_flow(socket, :process_order, params, as: :order_run)
  {:noreply, socket}
end

def handle_event("send_email", params, socket) do
  {:ok, socket} = LiveClient.enqueue(socket, MyApp.Jobs.SendEmail, params, as: :email_run)
  {:noreply, socket}
end
```

Then access `@order_run` and `@email_run` independently in templates.

## The Run Struct

`start_flow/4`, `enqueue/4`, and `subscribe/3` all assign a `%PgFlow.Schema.Run{}` struct with preloaded step states. The struct is updated in-memory as PubSub events arrive — no additional DB queries after the initial load.

### `%PgFlow.Schema.Run{}`

| Field             | Type        | Description                                      |
|-------------------|-------------|--------------------------------------------------|
| `run_id`          | `binary_id` | UUID of the run                                  |
| `flow_slug`       | `string`    | Flow or job slug                                 |
| `status`          | `string`    | `"started"`, `"completed"`, or `"failed"`        |
| `input`           | `map`       | Input data passed to the flow                    |
| `output`          | `map`       | Final output (set on completion)                 |
| `remaining_steps` | `integer`   | Steps not yet completed                          |
| `started_at`      | `datetime`  | When the run started                             |
| `completed_at`    | `datetime`  | When the run completed                           |
| `failed_at`       | `datetime`  | When the run failed                              |
| `step_states`     | `list`      | Preloaded `%StepState{}` structs                 |

### `%PgFlow.Schema.StepState{}`

| Field             | Type       | Description                                              |
|-------------------|------------|----------------------------------------------------------|
| `step_slug`       | `string`   | Step identifier                                          |
| `status`          | `string`   | `"created"`, `"started"`, `"completed"`, or `"failed"`   |
| `remaining_tasks` | `integer`  | Tasks not yet completed                                  |
| `output`          | `map`      | Step output (set on completion)                          |
| `error_message`   | `string`   | Error message (set on failure)                           |
| `started_at`      | `datetime` | When the step started                                    |
| `completed_at`    | `datetime` | When the step completed                                  |
| `failed_at`       | `datetime` | When the step failed                                     |

## Events

LiveClient handles the following PubSub events automatically:

| Event              | Effect                                                  |
|--------------------|---------------------------------------------------------|
| `:task_started`    | Updates step status to `"started"`                      |
| `:task_completed`  | Updates step status to `"completed"`, sets output       |
| `:task_failed`     | Updates step status to `"failed"`, sets error message   |
| `:run_completed`   | Updates run status to `"completed"`, sets output        |
| `:run_failed`      | Updates run status to `"failed"`                        |

Updates are idempotent — status only advances forward (`created` < `started` < `completed` < `failed`). Out-of-order or duplicate messages are safely ignored.

## Subscribing to Existing Runs

For pages that display a run started elsewhere (e.g., a run detail page), use `subscribe/3`:

```elixir
def mount(%{"run_id" => run_id}, _session, socket) do
  socket =
    socket
    |> LiveClient.init(pubsub: MyApp.PubSub)
    |> LiveClient.subscribe(run_id)

  {:ok, socket}
end
```
