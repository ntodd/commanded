# System Architecture

This guide explains the overall system architecture of a Commanded application, including the supervision tree, how events flow after persistence, how components communicate, and the role of PubSub in consistency coordination.

## Supervision Tree

When you start a Commanded application, `Commanded.Application.Supervisor` builds the following supervision tree:

```
MyApp (Commanded Application)
├── Event Store Adapter
│   └── (e.g., InMemory GenServer + SubscriptionsSupervisor)
├── PubSub Adapter
│   └── (e.g., LocalPubSub using Registry)
├── Registry Adapter
│   └── (e.g., LocalRegistry using Elixir Registry)
├── Task.Supervisor (Commands.TaskDispatcher)
│   └── (spawns tasks for command execution)
├── Aggregates.Supervisor (DynamicSupervisor)
│   ├── Aggregate GenServer (BankAccount, "acc-123")
│   ├── Aggregate GenServer (BankAccount, "acc-456")
│   └── ... (one per active aggregate instance)
├── Subscriptions.Registry (ETS table owner)
│   └── (tracks strongly consistent handlers)
└── Subscriptions (GenServer)
    └── (coordinates strong consistency)
```

### Supervisor Initialization

The `Commanded.Application.Supervisor` (`lib/commanded/application/supervisor.ex:47-66`) initializes children:

```elixir
def init({application, otp_app, config, name, opts}) do
  case runtime_config(application, otp_app, config, opts) do
    {:ok, config} ->
      {event_store_child_spec, config} = event_store_child_spec(name, config)
      {pubsub_child_spec, config} = pubsub_child_spec(name, config)
      {registry_child_spec, config} = registry_child_spec(name, config)

      children =
        event_store_child_spec ++
          pubsub_child_spec ++
          registry_child_spec ++
          app_child_spec(name, config)

      Supervisor.init(children, strategy: :one_for_one)
  end
end
```

### Application Children

The `app_child_spec/2` function (`lib/commanded/application/supervisor.ex:69-88`) creates:

```elixir
defp app_child_spec(name, config) do
  [
    {Task.Supervisor, name: task_dispatcher_name},           # For async command execution
    {Commanded.Aggregates.Supervisor, ...},                  # DynamicSupervisor for aggregates
    {Commanded.Subscriptions.Registry, ...},                 # ETS table for handler registration
    {Commanded.Subscriptions, ...}                           # Strong consistency coordination
  ]
end
```

## Who Saves Events?

Events are saved by the **Aggregate GenServer** calling the **Event Store adapter**.

### Flow

```
Aggregate GenServer
    │
    ▼
aggregate.ex:594-614 (append_to_stream/3)
    │
    ▼
Commanded.EventStore.append_to_stream/4
    │
    ▼
Event Store Adapter (e.g., InMemory, PostgreSQL EventStore)
    │
    ▼
Persist events atomically
    │
    ▼
Notify subscribers
```

### In the Aggregate

```elixir
# lib/commanded/aggregates/aggregate.ex:594-614
defp append_to_stream(pending_events, %ExecutionContext{} = context, %Aggregate{} = state) do
  %Aggregate{application: application, aggregate_uuid: aggregate_uuid, aggregate_version: expected_version} = state
  %ExecutionContext{causation_id: causation_id, correlation_id: correlation_id, metadata: metadata} = context

  event_data = Mapper.map_to_event_data(pending_events,
    causation_id: causation_id,
    correlation_id: correlation_id,
    metadata: metadata
  )

  # Calls the event store adapter
  EventStore.append_to_stream(application, aggregate_uuid, expected_version, event_data)
end
```

### Event Store Facade

The `Commanded.EventStore` module (`lib/commanded/event_store.ex`) delegates to the configured adapter:

```elixir
def append_to_stream(application, stream_uuid, expected_version, events, opts \\ []) do
  {adapter, adapter_meta} = Application.event_store_adapter(application)

  adapter.append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts)
end
```

## Event Flow After Persistence

After events are persisted, they flow to subscribers through two mechanisms:

### 1. Transient Subscriptions (Aggregate Self-Subscription)

When an aggregate process starts, it subscribes to its own stream to catch events appended by other processes:

```elixir
# lib/commanded/aggregates/aggregate.ex:294-299
def handle_continue(:subscribe_to_events, %Aggregate{} = state) do
  %Aggregate{application: application, aggregate_uuid: aggregate_uuid} = state

  :ok = EventStore.subscribe(application, aggregate_uuid)

  {:noreply, state}
end
```

The event store sends `{:events, events}` messages directly to the subscriber process.

### 2. Persistent Subscriptions (Event Handlers & Process Managers)

Event handlers create persistent subscriptions that survive restarts:

```elixir
# Event handler subscribes on start
EventStore.subscribe_to(application, :all, handler_name, self(), start_from, opts)
```

The event store tracks the last acknowledged event and resumes from there on restart.

## Complete Event Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. Command dispatched to Aggregate                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. Aggregate.execute/2 returns events                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  3. Events persisted to Event Store                                         │
│     EventStore.append_to_stream(app, stream_uuid, version, events)          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┴───────────────────────┐
            ▼                                               ▼
┌───────────────────────────────┐           ┌───────────────────────────────┐
│  4a. Transient Subscribers    │           │  4b. Persistent Subscriptions │
│  (Other aggregate instances)  │           │  (Event handlers, PMs)        │
│                               │           │                               │
│  send(pid, {:events, events}) │           │  send(pid, {:events, events}) │
└───────────────────────────────┘           └───────────────────────────────┘
                                                            │
                                                            ▼
                                            ┌───────────────────────────────┐
                                            │  5. Handler.handle/2 called   │
                                            │  - Update read model          │
                                            │  - Dispatch commands (PM)     │
                                            └───────────────────────────────┘
                                                            │
                                                            ▼
                                            ┌───────────────────────────────┐
                                            │  6. Acknowledge event         │
                                            │  - EventStore.ack_event       │
                                            │  - Subscriptions.ack_event    │
                                            └───────────────────────────────┘
                                                            │
                                                            ▼
                                            ┌───────────────────────────────┐
                                            │  7. PubSub broadcasts ack     │
                                            │  (for strong consistency)     │
                                            └───────────────────────────────┘
```

## PubSub Role

PubSub is used for **internal coordination**, not for delivering events to handlers. Its primary role is strong consistency coordination.

### PubSub Adapters

- **LocalPubSub** (default) - Uses Elixir's `Registry` for single-node deployments
- **PhoenixPubSub** - Uses Phoenix.PubSub for distributed clusters

### Strong Consistency Coordination

When an event handler with `:strong` consistency processes an event, it broadcasts an acknowledgment:

```elixir
# lib/commanded/subscriptions.ex:39-42
def ack_event(application, name, :strong, %RecordedEvent{} = event) do
  %RecordedEvent{stream_id: stream_id, stream_version: stream_version} = event

  PubSub.broadcast(application, @ack_topic, {:ack_event, name, stream_id, stream_version})
end
```

The `Subscriptions` GenServer listens for these broadcasts and tracks which handlers have processed which events.

## Strong Consistency Flow

When you dispatch with `consistency: :strong`:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. Dispatch command with consistency: :strong                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. Command executed, events persisted                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  3. ConsistencyGuarantee middleware (after_dispatch)                        │
│     Subscriptions.wait_for(app, aggregate_uuid, version, opts)              │
│     - Blocks until all :strong handlers have acked                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┴───────────────────────┐
            ▼                                               │
┌───────────────────────────────┐                          │
│  4. Event handlers process    │                          │
│     events and ack via PubSub │                          │
│                               │                          │
│  Subscriptions.ack_event(     │                          │
│    app, name, :strong, event) │                          │
└───────────────────────────────┘                          │
            │                                               │
            ▼                                               │
┌───────────────────────────────┐                          │
│  5. Subscriptions GenServer   │                          │
│     receives {:ack_event,...} │                          │
│     via PubSub subscription   │◀─────────────────────────┘
│                               │      (notifies waiter)
│  Tracks in ETS, notifies      │
│  waiting dispatcher           │
└───────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  6. Dispatcher unblocks, returns :ok to caller                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Subscriptions GenServer

The `Commanded.Subscriptions` module (`lib/commanded/subscriptions.ex`) coordinates strong consistency:

```elixir
def init(opts) do
  application = Keyword.fetch!(opts, :application)

  # Subscribe to ack broadcasts
  :ok = PubSub.subscribe(application, @ack_topic)

  {:ok, initial_state(application)}
end

def handle_info({:ack_event, name, stream_uuid, stream_version}, %Subscriptions{} = state) do
  # Track the ack in ETS
  :ets.insert(streams_table, {{name, stream_uuid}, stream_version, inserted_at_epoch})

  # Notify any waiting dispatchers
  state = %Subscriptions{state | subscribers: notify_subscribers(stream_uuid, state)}

  {:noreply, state}
end
```

### ConsistencyGuarantee Middleware

The middleware blocks after dispatch until handlers catch up:

```elixir
# lib/commanded/middleware/consistency_guarantee.ex:29-52
def after_dispatch(%Pipeline{} = pipeline) do
  %Pipeline{
    application: application,
    consistency: consistency,
    assigns: %{aggregate_uuid: aggregate_uuid, aggregate_version: aggregate_version}
  } = pipeline

  case Subscriptions.wait_for(application, aggregate_uuid, aggregate_version, opts) do
    :ok -> pipeline
    {:error, :timeout} -> respond(pipeline, {:error, :consistency_timeout})
  end
end
```

## How Components Communicate

### Aggregates Don't Talk to Each Other

Aggregates are isolated consistency boundaries. They communicate **only** through:
1. Events (one aggregate publishes, another's handler reacts)
2. Process managers (coordinate multi-aggregate workflows)

### Event Handlers Receive Events via Subscription

Event handlers don't poll—they receive `{:events, events}` messages from their event store subscription:

```elixir
# lib/commanded/event/handler.ex:954-977
def handle_info({:events, events}, state) do
  %Handler{application: application} = state

  state =
    events
    |> Upcast.upcast_event_stream(additional_metadata: %{application: application})
    |> Enum.reduce(state, &handle_event/2)

  {:noreply, state}
end
```

### Aggregates Receive External Events via Transient Subscription

If events are appended to an aggregate's stream by another process (rare), the aggregate receives them:

```elixir
# lib/commanded/aggregates/aggregate.ex:380-400
def handle_info({:events, events}, %Aggregate{} = state) do
  state =
    events
    |> Enum.reject(&event_already_seen?(&1, state))
    |> Upcast.upcast_event_stream(...)
    |> Enum.reduce(state, &handle_event/2)

  {:noreply, state}
end
```

## GenServer Communication Summary

| From | To | Mechanism |
|------|-----|-----------|
| Dispatcher | Aggregate | `GenServer.call` via Task |
| Aggregate | Event Store | `GenServer.call` (append_to_stream) |
| Event Store | Aggregate | `send(pid, {:events, events})` (transient sub) |
| Event Store | Event Handler | `send(pid, {:events, events})` (persistent sub) |
| Event Handler | Subscriptions | PubSub broadcast (ack_event) |
| Subscriptions | Dispatcher | `send(pid, {:ok, stream, version})` (unblock wait_for) |

## Pluggable Adapters

All infrastructure components are pluggable:

### Event Store Adapters

- `Commanded.EventStore.Adapters.InMemory` - For testing
- `Commanded.EventStore.Adapters.EventStore` - PostgreSQL (separate package)
- `Commanded.EventStore.Adapters.Extreme` - EventStoreDB (separate package)

### PubSub Adapters

- `Commanded.PubSub.LocalPubSub` - Single node (default)
- `Commanded.PubSub.PhoenixPubSub` - Distributed via Phoenix.PubSub

### Registry Adapters

- `Commanded.Registration.LocalRegistry` - Single node (default)
- `Commanded.Registration.GlobalRegistry` - Distributed via `:global`

## InMemory Event Store Internals

The InMemory adapter (`lib/commanded/event_store/adapters/in_memory.ex`) shows how events flow:

```elixir
defp persist_events(%State{} = state, stream_uuid, existing_events, new_events) do
  # ... create RecordedEvent structs ...

  # Store in state
  state = %State{state |
    streams: Map.put(streams, stream_uuid, stream_events),
    persisted_events: prepend(persisted_events, new_events)
  }

  # Publish to transient subscribers (e.g., aggregate self-subscription)
  state = publish_to_transient_subscribers(state, :all, publish_all_events)
  state = publish_to_transient_subscribers(state, stream_uuid, publish_stream_events)

  # Publish to persistent subscriptions (event handlers)
  persistent_subscriptions =
    Enum.into(persistent_subscriptions, %{}, fn {name, subscription} ->
      {name, publish_events(state, subscription)}
    end)

  {:ok, state}
end

defp publish_to_transient_subscribers(%State{} = state, stream_uuid, events) do
  subscribers = Map.get(transient_subscribers, stream_uuid, [])

  for subscriber <- subscribers do
    send(subscriber, {:events, events})  # Direct message to subscriber
  end

  state
end
```

## Key Source Files

| File | Purpose |
|------|---------|
| `lib/commanded/application/supervisor.ex` | Application supervision tree setup |
| `lib/commanded/event_store.ex` | Event store facade |
| `lib/commanded/event_store/adapters/in_memory.ex` | In-memory event store implementation |
| `lib/commanded/pubsub.ex` | PubSub facade |
| `lib/commanded/subscriptions.ex` | Strong consistency coordination |
| `lib/commanded/subscriptions/registry.ex` | Handler registration tracking |
| `lib/commanded/event/handler.ex` | Event handler implementation |
| `lib/commanded/middleware/consistency_guarantee.ex` | Blocks for strong consistency |
