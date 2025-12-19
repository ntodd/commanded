# Aggregate Internals

This guide explains the internal architecture of how aggregates work in Commanded. It covers how the GenServer hosts aggregate state, calls your module's callbacks, rebuilds state from events, and handles the `Multi` helper.

## Overview

A key insight in Commanded is the **separation between your aggregate module and the GenServer that hosts it**:

- **Your aggregate module** (e.g., `BankAccount`) is a plain Elixir module with a struct and pure functions
- **`Commanded.Aggregates.Aggregate`** is a GenServer that holds an instance of your struct and calls your functions

```
┌─────────────────────────────────────────────────────────────────────┐
│  Commanded.Aggregates.Aggregate (GenServer)                         │
│  ┌───────────────────────────────────────────────────────────────┐ │
│  │ %Aggregate{                                                    │ │
│  │   aggregate_module: BankAccount,        # Module atom          │ │
│  │   aggregate_state: %BankAccount{        # Struct instance      │ │
│  │     account_number: "123",                                     │ │
│  │     balance: 100                                               │ │
│  │   },                                                           │ │
│  │   aggregate_version: 5                                         │ │
│  │ }                                                              │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  On command:                                                        │
│    Kernel.apply(BankAccount, :execute, [%BankAccount{...}, cmd])   │
│                                                                     │
│  On event replay:                                                   │
│    BankAccount.apply(%BankAccount{...}, event)                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  BankAccount (Plain Module - NOT a GenServer)                       │
│                                                                     │
│  defmodule BankAccount do                                           │
│    defstruct [:account_number, :balance]  # Just data              │
│                                                                     │
│    def execute(state, command) -> events  # Pure function          │
│    def apply(state, event) -> new_state   # Pure function          │
│  end                                                                │
└─────────────────────────────────────────────────────────────────────┘
```

## The Aggregate GenServer State

The `Commanded.Aggregates.Aggregate` GenServer maintains its own state struct that wraps your aggregate:

```elixir
# lib/commanded/aggregates/aggregate.ex:134-142
defstruct [
  :application,
  :aggregate_module,      # The module atom (e.g., BankAccount)
  :aggregate_uuid,        # Unique identity string
  :aggregate_state,       # Instance of your struct (e.g., %BankAccount{...})
  :snapshotting,          # Snapshot configuration
  aggregate_version: 0,   # Current event stream version
  lifespan_timeout: :infinity
]
```

Key distinction:
- `aggregate_module` = `BankAccount` (the module atom, used with `Kernel.apply/3`)
- `aggregate_state` = `%BankAccount{account_number: "123", balance: 100}` (a struct instance holding domain data)

## Aggregate Process Lifecycle

### Starting the Process

When a command is dispatched, the `Aggregates.Supervisor` starts or locates the aggregate process:

```elixir
# lib/commanded/aggregates/supervisor.ex:27-55
def open_aggregate(application, aggregate_module, aggregate_uuid) do
  supervisor_name = Module.concat([application, __MODULE__])
  aggregate_name = Aggregate.name(application, aggregate_module, aggregate_uuid)

  args = [
    application: application,
    aggregate_module: aggregate_module,
    aggregate_uuid: aggregate_uuid
  ]

  Registration.start_child(application, aggregate_name, supervisor_name, {Aggregate, args})
end
```

### GenServer Initialization

On init, the GenServer schedules state population:

```elixir
# lib/commanded/aggregates/aggregate.ex:276-279
def init(%Aggregate{} = state) do
  {:ok, state, {:continue, :populate_aggregate_state}}
end
```

### State Population

The `handle_continue` callback triggers state rebuilding:

```elixir
# lib/commanded/aggregates/aggregate.ex:284-289
def handle_continue(:populate_aggregate_state, %Aggregate{} = state) do
  state = AggregateStateBuilder.populate(state)
  {:noreply, state, {:continue, :subscribe_to_events}}
end
```

## AggregateStateBuilder

The `AggregateStateBuilder` module (`lib/commanded/aggregates/aggregate_state_builder.ex`) reconstructs aggregate state from snapshots and events.

### populate/1

```elixir
# lib/commanded/aggregates/aggregate_state_builder.ex:44-62
def populate(%Aggregate{} = state) do
  %Aggregate{aggregate_module: aggregate_module, snapshotting: snapshotting} = state

  aggregate =
    case Snapshotting.read_snapshot(snapshotting) do
      {:ok, %SnapshotData{source_version: source_version, data: data}} ->
        # Snapshot found - use as starting point
        %Aggregate{state | aggregate_version: source_version, aggregate_state: data}

      {:error, _error} ->
        # No snapshot - start with empty struct
        %Aggregate{state | aggregate_version: 0, aggregate_state: struct(aggregate_module)}
    end

  # Replay events from snapshot version (or beginning)
  rebuild_from_events(aggregate)
end
```

### rebuild_from_events/1

Events are streamed from the event store and applied to rebuild state:

```elixir
# lib/commanded/aggregates/aggregate_state_builder.ex:67-87
def rebuild_from_events(%Aggregate{} = state) do
  %Aggregate{
    application: application,
    aggregate_uuid: aggregate_uuid,
    aggregate_version: aggregate_version
  } = state

  case EventStore.stream_forward(application, aggregate_uuid, aggregate_version + 1, @read_event_batch_size) do
    {:error, :stream_not_found} ->
      state  # New aggregate, no events yet

    event_stream ->
      rebuild_from_event_stream(event_stream, state)
  end
end
```

### Event Replay

Each event is applied by calling your aggregate module's `apply/2` function:

```elixir
# lib/commanded/aggregates/aggregate_state_builder.ex:90-111
defp rebuild_from_event_stream(event_stream, %Aggregate{} = state) do
  {state, count} =
    Enum.reduce(event_stream, {state, 0}, fn event, {state, count} ->
      %RecordedEvent{data: data, stream_version: stream_version} = event
      %Aggregate{aggregate_module: aggregate_module, aggregate_state: aggregate_state} = state

      state = %Aggregate{
        state
        | aggregate_version: stream_version,
          # Call YOUR module's apply/2 function
          aggregate_state: aggregate_module.apply(aggregate_state, data)
      }

      {state, count + 1}
    end)

  state
end
```

## Command Execution

When the GenServer receives a command, it calls your module's function using `Kernel.apply/3`.

### handle_call for Commands

```elixir
# lib/commanded/aggregates/aggregate.ex:316-360
def handle_call({:execute_command, %ExecutionContext{} = context}, from, %Aggregate{} = state) do
  %ExecutionContext{lifespan: lifespan, command: command} = context

  # Execute the command
  {result, state} = execute_command(context, state)

  # Determine lifespan timeout based on result
  lifespan_timeout =
    case result do
      {:ok, []} -> aggregate_lifespan_timeout(lifespan, :after_command, command)
      {:ok, events} -> aggregate_lifespan_timeout(lifespan, :after_event, events)
      {:error, error} -> aggregate_lifespan_timeout(lifespan, :after_error, error)
    end

  # Format and return reply
  formatted_reply = ExecutionContext.format_reply(result, context, state)
  reply_with_lifespan(formatted_reply, state)
end
```

### execute_command/2

This is where your aggregate's function is called:

```elixir
# lib/commanded/aggregates/aggregate.ex:502-541
defp execute_command(%ExecutionContext{} = context, %Aggregate{} = state) do
  %ExecutionContext{command: command, handler: handler, function: function} = context
  %Aggregate{aggregate_state: aggregate_state} = state

  # handler = BankAccount (module), function = :execute, aggregate_state = %BankAccount{...}
  case Kernel.apply(handler, function, [aggregate_state, command]) do
    {:error, _error} = reply ->
      {reply, state}

    none when none in [:ok, nil, []] ->
      {{:ok, []}, state}

    %Multi{} = multi ->
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
```

The key line is:
```elixir
Kernel.apply(handler, function, [aggregate_state, command])
```

Which translates to calling:
```elixir
BankAccount.execute(%BankAccount{account_number: "123", balance: 100}, %DepositMoney{amount: 50})
```

## Aggregate Callbacks

Your aggregate module defines two callbacks (pure functions):

### execute/2 - Command Handler

Receives current state and command, returns events or error:

```elixir
defmodule BankAccount do
  defstruct [:account_number, :balance]

  def execute(%BankAccount{account_number: nil}, %OpenAccount{} = cmd) do
    %BankAccountOpened{
      account_number: cmd.account_number,
      initial_balance: cmd.initial_balance
    }
  end

  def execute(%BankAccount{}, %OpenAccount{}) do
    {:error, :account_already_opened}
  end

  def execute(%BankAccount{balance: balance}, %WithdrawMoney{amount: amount})
      when amount <= balance do
    %MoneyWithdrawn{amount: amount, new_balance: balance - amount}
  end

  def execute(%BankAccount{}, %WithdrawMoney{}) do
    {:error, :insufficient_funds}
  end
end
```

**Valid return values:**

| Return | Meaning |
|--------|---------|
| `%Event{}` | Single event |
| `[%Event{}, ...]` | Multiple events |
| `{:ok, %Event{}}` | Single event (explicit ok) |
| `{:ok, [%Event{}, ...]}` | Multiple events (explicit ok) |
| `:ok`, `nil`, `[]` | No events (command accepted, no state change) |
| `{:error, reason}` | Business rule violation |
| `%Multi{}` | Multiple events with intermediate state updates |

### apply/2 - State Mutator

Receives current state and event, returns new state:

```elixir
def apply(%BankAccount{} = state, %BankAccountOpened{} = event) do
  %BankAccount{state |
    account_number: event.account_number,
    balance: event.initial_balance
  }
end

def apply(%BankAccount{} = state, %MoneyWithdrawn{} = event) do
  %BankAccount{state | balance: event.new_balance}
end
```

**Important**: `apply/2` must **never fail**. It's called during event replay to rebuild state. You cannot reject an event that has already occurred.

### The Behaviour is Optional

The `Commanded.Aggregates.Aggregate` behaviour provides typespecs but is not required:

```elixir
# lib/commanded/aggregates/aggregate.ex:124-132
@callback execute(aggregate :: state(), command :: struct()) ::
            return_event() | no_return_event() | {:error, term()}

@callback apply(aggregate :: state(), event :: struct()) :: state()

@optional_callbacks execute: 2
```

Commanded calls whatever function is configured in the router (`:execute` by default).

## Event Persistence

After your `execute/2` returns events, they're applied and persisted:

### apply_and_persist_events/3

```elixir
# lib/commanded/aggregates/aggregate.ex:543-550
defp apply_and_persist_events(pending_events, context, %Aggregate{} = state) do
  %Aggregate{aggregate_module: aggregate_module, aggregate_state: aggregate_state} = state

  pending_events = List.wrap(pending_events)
  # Apply events to state BEFORE persisting
  aggregate_state = apply_events(aggregate_module, aggregate_state, pending_events)

  persist_events(pending_events, aggregate_state, context, state)
end

defp apply_events(aggregate_module, aggregate_state, events) do
  Enum.reduce(events, aggregate_state, &aggregate_module.apply(&2, &1))
end
```

### persist_events/4

```elixir
# lib/commanded/aggregates/aggregate.ex:556-590
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
      # Concurrent modification - rebuild state and retry
      state = AggregateStateBuilder.rebuild_from_events(state)
      case ExecutionContext.retry(context) do
        {:ok, context} -> execute_command(context, state)
        reply -> {reply, state}
      end
  end
end
```

## Commanded.Aggregate.Multi

For commands that emit multiple events where later events depend on state changes from earlier events, use `Multi`:

```elixir
def execute(%BankAccount{state: :active} = account, %WithdrawMoney{amount: amount}) do
  account
  |> Multi.new()
  |> Multi.execute(&withdraw_money(&1, amount))  # Returns MoneyWithdrawn
  |> Multi.execute(&check_balance/1)              # Uses UPDATED balance to check overdraft
end

defp withdraw_money(%BankAccount{} = account, amount) do
  %MoneyWithdrawn{amount: amount, balance: account.balance - amount}
end

defp check_balance(%BankAccount{balance: balance}) when balance < 0 do
  %AccountOverdrawn{balance: balance}
end
defp check_balance(%BankAccount{}), do: []
```

### How Multi.run/1 Works

```elixir
# lib/commanded/aggregates/multi.ex:162-215
def run(%Multi{aggregate: aggregate, executions: executions}) do
  executions
  |> Enum.reverse()
  |> Enum.reduce({aggregate, %{}, []}, fn {step_name, execute_fun}, {aggregate, steps, events} ->
    case execute_function(execute_fun, aggregate, steps) do
      {:error, _reason} = error ->
        throw(error)  # Stop and return error

      pending_events ->
        pending_events = List.wrap(pending_events)
        # Apply events to get UPDATED state for next step
        evolved_aggregate = apply_events(aggregate, pending_events)
        {evolved_aggregate, updated_steps, events ++ pending_events}
    end
  end)
end

defp apply_events(aggregate, events) do
  Enum.reduce(events, aggregate, &aggregate.__struct__.apply(&2, &1))
end
```

The key insight is that `Multi` applies events between steps, so each subsequent function sees the updated aggregate state.

## Why This Design?

1. **Pure functions** - Your aggregate logic is pure functions (state, input) → (events, new state). Easy to test without processes.

2. **Process per aggregate instance** - One GenServer per unique `{module, uuid}`. Commands are serialized through this process, ensuring consistency.

3. **Separation of concerns** - Your domain logic knows nothing about GenServers, event stores, or persistence. Commanded handles infrastructure.

4. **Event sourcing** - State is always derived from events. The GenServer rebuilds state on startup by replaying events through your `apply/2`.

## Key Source Files

| File | Purpose |
|------|---------|
| `lib/commanded/aggregates/aggregate.ex` | GenServer that hosts aggregate state and executes commands |
| `lib/commanded/aggregates/aggregate_state_builder.ex` | Rebuilds state from snapshots and events |
| `lib/commanded/aggregates/multi.ex` | Helper for multiple dependent events |
| `lib/commanded/aggregates/supervisor.ex` | DynamicSupervisor managing aggregate processes |
| `lib/commanded/aggregates/execution_context.ex` | Command execution metadata and retry logic |
