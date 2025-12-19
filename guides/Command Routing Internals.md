# Command Routing Internals

This guide explains the internal architecture of how commands are routed and executed in Commanded. It traces the complete flow from command dispatch to event persistence.

## Overview

When you dispatch a command, it flows through several layers:

1. **Router** - Pattern matches on command type, builds dispatch payload
2. **Dispatcher** - Orchestrates middleware and aggregate execution
3. **Middleware Pipeline** - Extracts identity, applies cross-cutting concerns
4. **Aggregate Supervisor** - Opens or locates aggregate process
5. **Aggregate GenServer** - Executes command, persists events

```
┌─────────────────────────────────────────────────────────────────────────┐
│  MyApp.dispatch(%CreateAccount{account_id: "123"})                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Router.do_dispatch/2                                                   │
│  - Pattern match on command struct                                      │
│  - Build Dispatcher.Payload                                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Dispatcher.dispatch/1                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Middleware Pipeline (before_dispatch)                           │   │
│  │  1. ExtractAggregateIdentity                                     │   │
│  │  2. ConsistencyGuarantee                                         │   │
│  │  3. Custom middleware...                                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Aggregates.Supervisor.open_aggregate/3                                 │
│  - Start or locate existing Aggregate GenServer                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Task.Supervisor.async_nolink → Aggregate.execute/5                     │
│  - GenServer.call(aggregate_pid, {:execute_command, context})           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Aggregate GenServer                                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  execute_command/2                                               │   │
│  │  - handler.function(aggregate_state, command)                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  persist_events/4                                                │   │
│  │  - EventStore.append_to_stream(...)                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Dispatcher (after_dispatch middleware)                                 │
│  - Return :ok or {:ok, result}                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Step 1: Router Registration (Compile-time)

When you define a router using `Commanded.Commands.Router`, the `dispatch` macro registers commands at compile time:

```elixir
defmodule BankRouter do
  use Commanded.Commands.Router

  dispatch OpenAccount, to: BankAccount, identity: :account_number
end
```

The macro generates a pattern-matched `do_dispatch/2` function clause for each registered command. This happens in `lib/commanded/commands/router.ex:507-591`.

Key configuration stored per command:
- `aggregate_module` - The aggregate to route to
- `handler_module` - The handler (or aggregate if dispatching directly)
- `function` - Function to call (`:execute` by default, or `:handle` for handlers)
- `identity` - Field or function to extract aggregate UUID
- `identity_prefix` - Optional prefix for stream identity
- `middleware` - List of middleware modules

## Step 2: Command Dispatch Entry

When you call `MyApp.dispatch(%OpenAccount{...})`, the generated `do_dispatch/2` function:

1. Merges dispatch options with registered defaults
2. Builds a `Commanded.Commands.Dispatcher.Payload` struct:

```elixir
# router.ex:568-587
payload = %Payload{
  application: application,
  command: command,
  command_uuid: command_uuid,
  causation_id: causation_id,
  correlation_id: correlation_id,
  consistency: consistency,
  handler_module: @handler,
  handler_function: @function,
  aggregate_module: @aggregate,
  identity: identity,
  identity_prefix: identity_prefix,
  timeout: timeout,
  lifespan: @lifespan,
  metadata: metadata,
  middleware: @middleware,
  retry_attempts: retry_attempts
}

Dispatcher.dispatch(payload)
```

## Step 3: Dispatcher Orchestration

The `Commanded.Commands.Dispatcher` module (`lib/commanded/commands/dispatcher.ex`) orchestrates the entire execution:

```elixir
# dispatcher.ex:42-64
def dispatch(%Payload{} = payload) do
  pipeline = to_pipeline(payload)

  # Emit telemetry start event
  start_time = telemetry_start(telemetry_metadata)

  # Run before_dispatch middleware
  pipeline = before_dispatch(pipeline, payload)

  unless Pipeline.halted?(pipeline) do
    context = to_execution_context(pipeline, payload)

    pipeline
    |> execute(payload, context)
    |> telemetry_stop(start_time, telemetry_metadata)
    |> Pipeline.response()
  else
    # Middleware halted the pipeline
    pipeline
    |> after_failure(payload)
    |> telemetry_stop(start_time, telemetry_metadata)
    |> Pipeline.response()
  end
end
```

## Step 4: Middleware Pipeline

The middleware pipeline (`lib/commanded/middleware/pipeline.ex`) provides hooks for cross-cutting concerns.

### Default Middleware

Two middleware modules are always included (`router.ex:249-252`):

1. **`ExtractAggregateIdentity`** - Extracts the aggregate UUID from the command
2. **`ConsistencyGuarantee`** - Enforces consistency settings

### ExtractAggregateIdentity

This middleware (`lib/commanded/middleware/extract_aggregate_identity.ex`) extracts the aggregate identity:

```elixir
# extract_aggregate_identity.ex:12-27
def before_dispatch(%Pipeline{} = pipeline) do
  with aggregate_uuid when aggregate_uuid not in [nil, ""] <- extract_aggregate_uuid(pipeline),
       aggregate_uuid when is_binary(aggregate_uuid) <- identity_to_string(aggregate_uuid),
       aggregate_uuid when is_binary(aggregate_uuid) <- prefix(aggregate_uuid, pipeline) do
    assign(pipeline, :aggregate_uuid, aggregate_uuid)
  else
    nil ->
      pipeline
      |> respond({:error, :invalid_aggregate_identity})
      |> halt()
  end
end
```

Identity extraction supports:
- **Field access**: `identity: :account_number` extracts `command.account_number`
- **Function**: `identity: &my_func/1` calls the function with the command

### Pipeline Chain Execution

Middleware is executed via `Pipeline.chain/3`:

```elixir
# pipeline.ex:116-118
def chain(%Pipeline{} = pipeline, stage, [module | modules]) do
  chain(apply(module, stage, [pipeline]), stage, modules)
end
```

If any middleware calls `Pipeline.halt/1`, execution stops and the error response is returned.

## Step 5: Open Aggregate Process

The dispatcher opens or locates the aggregate GenServer (`dispatcher.ex:88-93`):

```elixir
{:ok, ^aggregate_uuid} =
  Commanded.Aggregates.Supervisor.open_aggregate(
    application,
    aggregate_module,
    aggregate_uuid
  )
```

### Aggregate Supervisor

The `Commanded.Aggregates.Supervisor` (`lib/commanded/aggregates/supervisor.ex`) is a `DynamicSupervisor` that manages aggregate processes:

```elixir
# supervisor.ex:27-55
def open_aggregate(application, aggregate_module, aggregate_uuid) do
  supervisor_name = Module.concat([application, __MODULE__])
  aggregate_name = Aggregate.name(application, aggregate_module, aggregate_uuid)

  args = [
    application: application,
    aggregate_module: aggregate_module,
    aggregate_uuid: aggregate_uuid
  ]

  case Registration.start_child(application, aggregate_name, supervisor_name, {Aggregate, args}) do
    {:ok, _pid} -> {:ok, aggregate_uuid}
    {:error, {:already_started, _pid}} -> {:ok, aggregate_uuid}
  end
end
```

The `Registration` module provides pluggable process registration (local or distributed).

## Step 6: Execute Command via Task

The dispatcher spawns an async task to execute the command (`dispatcher.ex:97-104`):

```elixir
task_dispatcher_name = Module.concat([application, Commanded.Commands.TaskDispatcher])

task =
  Task.Supervisor.async_nolink(task_dispatcher_name, Aggregate, :execute, [
    application,
    aggregate_module,
    aggregate_uuid,
    context,
    timeout
  ])

result =
  case Task.yield(task, timeout) || Task.shutdown(task) do
    {:ok, result} -> result
    {:exit, _reason} -> {:error, :aggregate_execution_failed}
    nil -> {:error, :aggregate_execution_timeout}
  end
```

Using `async_nolink` ensures the dispatcher doesn't crash if the aggregate fails.

## Step 7: Aggregate GenServer

The `Commanded.Aggregates.Aggregate` module (`lib/commanded/aggregates/aggregate.ex`) is a GenServer that:

1. Loads state from snapshots and/or events on init
2. Executes commands serially
3. Persists events to the event store
4. Applies events to update state

### Aggregate.execute/5

The public API for command execution (`aggregate.ex:197-217`):

```elixir
def execute(application, aggregate_module, aggregate_uuid, %ExecutionContext{} = context, timeout) do
  name = via_name(application, aggregate_module, aggregate_uuid)

  try do
    GenServer.call(name, {:execute_command, context}, timeout)
  catch
    :exit, {:noproc, _} -> {:exit, {:normal, :aggregate_stopped}}
    :exit, {:normal, _} -> {:exit, {:normal, :aggregate_stopped}}
  end
end
```

### handle_call for Commands

The GenServer handles command execution (`aggregate.ex:316-360`):

```elixir
def handle_call({:execute_command, %ExecutionContext{} = context}, from, %Aggregate{} = state) do
  %ExecutionContext{lifespan: lifespan, command: command} = context

  # Emit telemetry
  start_time = telemetry_start(telemetry_metadata)

  # Execute the command
  {result, state} = execute_command(context, state)

  # Determine lifespan timeout based on result
  lifespan_timeout = case result do
    {:ok, []} -> aggregate_lifespan_timeout(lifespan, :after_command, command)
    {:ok, events} -> aggregate_lifespan_timeout(lifespan, :after_event, events)
    {:error, error} -> aggregate_lifespan_timeout(lifespan, :after_error, error)
  end

  # Format and return reply
  formatted_reply = ExecutionContext.format_reply(result, context, state)
  {:reply, formatted_reply, state, lifespan_timeout}
end
```

### execute_command/2

The actual command execution logic (`aggregate.ex:502-541`):

```elixir
defp execute_command(%ExecutionContext{} = context, %Aggregate{} = state) do
  %ExecutionContext{command: command, handler: handler, function: function} = context
  %Aggregate{aggregate_state: aggregate_state} = state

  with :ok <- before_execute_command(aggregate_state, context) do
    case Kernel.apply(handler, function, [aggregate_state, command]) do
      {:error, _error} = reply ->
        {reply, state}

      none when none in [:ok, nil, []] ->
        {{:ok, []}, state}

      %Multi{} = multi ->
        # Handle Commanded.Aggregate.Multi for multiple events
        case Multi.run(multi) do
          {:error, _error} = reply -> {reply, state}
          {aggregate_state, pending_events} ->
            persist_events(pending_events, aggregate_state, context, state)
        end

      {:ok, pending_events} ->
        apply_and_persist_events(pending_events, context, state)

      pending_events ->
        apply_and_persist_events(pending_events, context, state)
    end
  end
end
```

## Step 8: Event Persistence

Events are persisted atomically to the event store (`aggregate.ex:556-590`):

```elixir
defp persist_events(pending_events, aggregate_state, context, state) do
  %Aggregate{aggregate_version: expected_version} = state

  with :ok <- append_to_stream(pending_events, context, state) do
    aggregate_version = expected_version + length(pending_events)

    state = %Aggregate{
      state
      | aggregate_state: aggregate_state,
        aggregate_version: aggregate_version
    }

    {{:ok, pending_events}, state}
  else
    {:error, :wrong_expected_version} ->
      # Fetch missing events and retry
      state = AggregateStateBuilder.rebuild_from_events(state)
      case ExecutionContext.retry(context) do
        {:ok, context} -> execute_command(context, state)
        reply -> {reply, state}
      end
  end
end
```

### append_to_stream/3

Events are mapped and appended (`aggregate.ex:594-614`):

```elixir
defp append_to_stream(pending_events, %ExecutionContext{} = context, %Aggregate{} = state) do
  %Aggregate{application: application, aggregate_uuid: aggregate_uuid, aggregate_version: expected_version} = state
  %ExecutionContext{causation_id: causation_id, correlation_id: correlation_id, metadata: metadata} = context

  event_data =
    Mapper.map_to_event_data(pending_events,
      causation_id: causation_id,
      correlation_id: correlation_id,
      metadata: metadata
    )

  EventStore.append_to_stream(application, aggregate_uuid, expected_version, event_data)
end
```

## Step 9: After Dispatch

Back in the dispatcher, successful execution flows through after_dispatch middleware (`dispatcher.ex:124-131`):

```elixir
{:ok, aggregate_version, events, aggregate_state} ->
  pipeline
  |> Pipeline.assign(:aggregate_version, aggregate_version)
  |> Pipeline.assign(:events, events)
  |> Pipeline.assign(:aggregate_state, aggregate_state)
  |> after_dispatch(payload)
  |> Pipeline.respond(:ok)
```

The final response is extracted via `Pipeline.response/1` and returned to the caller.

## Error Handling and Retries

### Wrong Expected Version

If the event store returns `{:error, :wrong_expected_version}` (concurrent modification), the aggregate:

1. Rebuilds state from the event store
2. Retries command execution (up to `retry_attempts` times)

### Aggregate Process Stopped

If the aggregate process stops during execution (e.g., lifespan timeout), the dispatcher can retry:

```elixir
# dispatcher.ex:141-147
{:exit, {:normal, :aggregate_stopped}} ->
  maybe_retry(pipeline, payload, context)

{:error, :remote_node_down} ->
  maybe_retry(pipeline, payload, context)
```

## Telemetry Events

The following telemetry events are emitted:

- `[:commanded, :application, :dispatch, :start]` - Command dispatch started
- `[:commanded, :application, :dispatch, :stop]` - Command dispatch completed
- `[:commanded, :aggregate, :execute, :start]` - Aggregate execution started
- `[:commanded, :aggregate, :execute, :stop]` - Aggregate execution completed
- `[:commanded, :aggregate, :execute, :exception]` - Aggregate raised an exception

## Key Source Files

| File | Purpose |
|------|---------|
| `lib/commanded/commands/router.ex` | Command routing DSL and dispatch entry point |
| `lib/commanded/commands/dispatcher.ex` | Orchestrates middleware and aggregate execution |
| `lib/commanded/middleware/pipeline.ex` | Middleware chain execution |
| `lib/commanded/middleware/extract_aggregate_identity.ex` | Extracts aggregate UUID from command |
| `lib/commanded/aggregates/supervisor.ex` | DynamicSupervisor for aggregate processes |
| `lib/commanded/aggregates/aggregate.ex` | GenServer that executes commands and persists events |
| `lib/commanded/aggregates/execution_context.ex` | Command execution metadata and retry logic |
