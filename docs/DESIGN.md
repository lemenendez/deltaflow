# DeltaFlow Design

> Status: v0.1 design.
>
> Goal: keep DeltaFlow small, durable, and clear.
>
> DeltaFlow v0.1 is a latest-state synchronization worker based on durable Deltas.

---

## 1. Goal

DeltaFlow v0.1 helps an application keep one derived system synchronized with the latest state of a business Projection.

The first version answers one question:

```text
Something changed. Can DeltaFlow project the latest business state and apply it safely?
```

The first version is intentionally small:

```text
one Sync
one Projector
one ProjectionApplier
latest_state mode only
durable Delta storage
worker leases
retries
dead Deltas
Delta Ghost handling
```

---

## 2. Non-goals for v0.1

DeltaFlow v0.1 does not include:

```text
CDC as the primary abstraction
multiple appliers per Sync
fan-out
target deliveries
connector registry
dynamic plugins
captured_input
captured_output
envelopes
codecs
Avro
Protobuf
Apache Beam
cloud control plane
billing
dashboard
non-idempotent effect targets
```

These are future ideas, not v0.1 requirements.

---

## 3. Core Vocabulary

### 3.1 Projection

A Projection is the business object or business representation that DeltaFlow synchronizes.

A Projection is not necessarily a database row. It may be built from one table, many tables, joins, external data, or business rules.

Examples:

```text
Contact
Order
EmployeeProfile
Invoice
Document
```

A Projection is the thing DeltaFlow wants to keep synchronized in a derived system.

---

### 3.2 Projection Type

A Projection Type is a string that classifies a Projection.

It acts as a classifier or typer.

Examples:

```text
"Contact"
"Order"
"EmployeeProfile"
"Invoice"
"Document"
```

In Go:

```go
type ProjectionType string
```

A Projection Type is logical. It should not be assumed to be a database table name.

Example:

```text
Projection Type: "Contact"
```

A `Contact` Projection may be built from:

```text
contacts
contact_emails
contact_phones
contact_addresses
contact_tags
```

---

### 3.3 Projection Key

A Projection Key identifies one Projection instance.

The key may be a single-field key or a structured composite key.

Examples:

```json
{ "contact_id": "123" }
```

```json
{
  "order_id": "ord_123",
  "line_number": 2
}
```

```json
{
  "tenant_id": "t_001",
  "user_id": "usr_999"
}
```

DeltaFlow treats Projection Keys as structured data, not as a single string-only identifier.

For storage and uniqueness, DeltaFlow computes a stable hash:

```text
projection_key_hash = sha256(canonical_json(projection_key))
```

Equivalent keys must produce the same hash regardless of field order.

---

### 3.4 Projection Identity

A Projection Identity names one Projection instance.

It is composed of:

```text
projection_type
projection_key
```

Example:

```text
projection_type = "Contact"
projection_key  = { "contact_id": "123" }
```

A Projection Identity answers:

```text
Which Projection instance are we talking about?
```

It does not answer:

```text
How do we build it?
Where do we apply it?
Which worker should process it?
Which retry policy applies?
```

Those concerns belong to the Sync and the worker wiring.

---

### 3.5 Projector

A Projector is user-provided code that obtains or builds a Projection.

A Projector may be an object, function, or interface.

A Projector answers:

```text
How do we get the latest Projection for this Projection Identity?
```

The verb is:

```text
Project
```

In v0.1, the Projector always projects the latest state.

Minimal interface:

```go
type Projector interface {
    Project(ctx context.Context, identity ProjectionIdentity) (Projection, error)
}
```

If the Projection no longer exists, the Projector returns:

```go
ErrProjectionNotFound
```

That case is called a Delta Ghost.

---

### 3.6 Projection Operation

A Projection Operation is the operation that the worker asks a ProjectionApplier to apply.

For v0.1, only two operations exist:

```text
upsert
delete
```

The worker decides the operation after asking the Projector for the latest Projection.

If the Projector returns a Projection:

```text
operation = upsert
```

If the Projector returns `ErrProjectionNotFound`:

```text
operation = delete
```

Go shape:

```go
type ProjectionOperationType string

const (
    ProjectionOpUpsert ProjectionOperationType = "upsert"
    ProjectionOpDelete ProjectionOperationType = "delete"
)

type ProjectionOperation struct {
    Type       ProjectionOperationType
    Identity   ProjectionIdentity
    Projection *Projection
}
```

For `delete`, `Projection` is nil.

---

### 3.7 ProjectionApplier

A ProjectionApplier is user-provided code that applies a Projection Operation to a derived system.

A ProjectionApplier is not the target system itself.

Example:

```text
Elasticsearch is the derived system.
ElasticsearchProjectionApplier is the code that applies operations to Elasticsearch.
```

Minimal interface:

```go
type ProjectionApplier interface {
    Apply(ctx context.Context, op ProjectionOperation) error
}
```

For v0.1, ProjectionAppliers are assumed to be idempotent.

That means:

```text
Apply upsert twice = safe
Apply delete twice = safe
Apply delete when missing = safe
```

Examples of v0.1-friendly derived systems:

```text
Elasticsearch document
Redis key
Postgres read model row
RAG chunks keyed by Projection Identity
```

---

### 3.8 Delta

A Delta is a durable signal that a Projection Identity changed and must be synchronized.

A Delta is not a historical domain event.

It does not promise to describe exactly what happened.

It means:

```text
This Projection Identity became dirty. Project its latest state and apply it.
```

Example Delta:

```json
{
  "sync_id": "contacts-to-elasticsearch",
  "projection_type": "Contact",
  "projection_key": {
    "contact_id": "123"
  }
}
```

A Delta does not mean:

```text
Contact 123 was inserted.
Contact 123 was updated.
Contact 123 was deleted.
```

It means:

```text
Synchronize the latest state of Contact 123.
```

---

### 3.9 Delta Outbox

The Delta Outbox is the durable database table where the application stores Deltas.

The application should insert a Delta in the same transaction as the business data change.

Example:

```sql
BEGIN;

UPDATE contacts
SET name = 'Leo'
WHERE id = '123';

INSERT INTO deltaflow_deltas (
    sync_id,
    projection_type,
    projection_key,
    projection_key_hash,
    state
)
VALUES (
    'contacts-to-elasticsearch',
    'Contact',
    '{"contact_id":"123"}',
    'sha256...',
    'pending'
);

COMMIT;
```

This is the outbox pattern.

The important rule:

```text
business write + Delta insert commit together
```

If the transaction rolls back, the Delta rolls back too.

---

### 3.10 Sync

A Sync is a v0.1 synchronization configuration.

It connects:

```text
one Projector
one ProjectionApplier
one Projection Type
```

A Sync answers:

```text
How should this kind of Projection be synchronized?
```

Example:

```text
sync_id = contacts-to-elasticsearch
projection_type = Contact
projector = ContactProjector
applier = ElasticsearchProjectionApplier
```

In v0.1, a Sync is not a graph, not a connector registry, not a multi-target fan-out system, and not an Apache Beam pipeline.

It is a small source-to-derived-system synchronization route.

---

### 3.11 SyncWorker

A SyncWorker is the runtime process that claims Deltas and executes a Sync.

A SyncWorker:

```text
claims pending Deltas
calls Projector.Project(identity)
creates a ProjectionOperation
calls ProjectionApplier.Apply(operation)
records success or failure
schedules retries
marks Deltas dead after retry exhaustion
```

The SyncWorker should be boring.

Boring ships.

---

### 3.12 Delta Ghost

A Delta Ghost is a Projection Identity that produced a Delta but no longer exists when latest-state synchronization runs.

Example:

```text
Contact / 1
insert -> update -> update -> delete
```

When the SyncWorker finally runs:

```text
Projector.Project(Contact, { "contact_id": "1" }) -> ErrProjectionNotFound
```

This is not an error.

In `latest_state` mode, the correct operation is usually:

```text
ProjectionOperation = delete
```

A Delta Ghost is a valid synchronization outcome.

---

## 4. Naming Convention

DeltaFlow v0.1 uses projection-based terminology.

Preferred internal names:

```text
projection_type
projection_key
projection_key_hash
sync_id
delta
```

Avoid these names in the core model:

```text
entity_type
entity_id
event
target
source
connector
```

Reason:

```text
entity_id implies a single scalar identifier.
event implies historical replay.
target confuses the external system with the code that applies operations.
source is too generic for the Projector role.
connector implies plugin/registry architecture.
```

Compatibility mapping:

```text
entity_type -> projection_type
entity_id   -> projection_key with one field
```

Example:

```text
entity_type = Contact
entity_id   = 123
```

Should be represented internally as:

```json
{
  "projection_type": "Contact",
  "projection_key": {
    "contact_id": "123"
  }
}
```

---

## 5. v0.1 Flow

```text
Application transaction
  1. write business data
  2. insert Delta into Delta Outbox
  3. commit

SyncWorker
  4. claim pending Delta
  5. call Projector.Project(identity)
  6. if Projection exists:
         create upsert operation
     if Projection does not exist:
         create delete operation
  7. call ProjectionApplier.Apply(operation)
  8. mark Delta synced, retrying, failed, or dead
```

---

## 6. Delta States

v0.1 uses one state machine for Deltas.

```text
pending
processing
synced
failed
retrying
dead
```

Definitions:

```text
pending
  Delta exists and is available to be claimed.

processing
  A SyncWorker has claimed the Delta.

synced
  The ProjectionApplier successfully applied the required operation.

failed
  The last attempt failed, but the Delta may retry.

retrying
  The Delta is waiting for a future retry.

dead
  The Delta exhausted retries or was manually marked dead.
```

No parent/child state model exists in v0.1.

There is no TargetDelivery in v0.1.

---

## 7. Database Table

v0.1 starts with one table.

```sql
CREATE TABLE deltaflow_deltas (
    id UUID PRIMARY KEY,

    sync_id TEXT NOT NULL,

    projection_type TEXT NOT NULL,
    projection_key JSONB NOT NULL,
    projection_key_hash TEXT NOT NULL,

    state TEXT NOT NULL,

    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,

    last_error TEXT NULL,
    last_error_code TEXT NULL,

    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    locked_by TEXT NULL,
    locked_until TIMESTAMPTZ NULL,

    ghost_detected BOOLEAN NOT NULL DEFAULT false,

    synced_at TIMESTAMPTZ NULL,
    dead_at TIMESTAMPTZ NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Suggested indexes:

```sql
CREATE INDEX ix_deltaflow_deltas_claim
ON deltaflow_deltas (state, available_at);

CREATE INDEX ix_deltaflow_deltas_projection
ON deltaflow_deltas (
    sync_id,
    projection_type,
    projection_key_hash
);
```

Optional dedupe index for latest-state compaction:

```sql
CREATE UNIQUE INDEX ux_deltaflow_pending_delta
ON deltaflow_deltas (
    sync_id,
    projection_type,
    projection_key_hash
)
WHERE state IN ('pending', 'retrying');
```

This prevents flooding the outbox with duplicate pending Deltas for the same Projection Identity.

---

## 8. Minimal Go Interfaces

```go
type ProjectionType string

type ProjectionKey map[string]json.RawMessage

type ProjectionIdentity struct {
    Type ProjectionType
    Key  ProjectionKey
}

type Projection struct {
    Identity  ProjectionIdentity
    Payload   []byte
    MediaType string
    Checksum  string
}

type Projector interface {
    Project(ctx context.Context, identity ProjectionIdentity) (Projection, error)
}

type ProjectionOperationType string

const (
    ProjectionOpUpsert ProjectionOperationType = "upsert"
    ProjectionOpDelete ProjectionOperationType = "delete"
)

type ProjectionOperation struct {
    Type       ProjectionOperationType
    Identity   ProjectionIdentity
    Projection *Projection
}

type ProjectionApplier interface {
    Apply(ctx context.Context, op ProjectionOperation) error
}
```

Function adapters may also be provided:

```go
type ProjectorFunc func(context.Context, ProjectionIdentity) (Projection, error)

func (f ProjectorFunc) Project(ctx context.Context, id ProjectionIdentity) (Projection, error) {
    return f(ctx, id)
}

type ProjectionApplierFunc func(context.Context, ProjectionOperation) error

func (f ProjectionApplierFunc) Apply(ctx context.Context, op ProjectionOperation) error {
    return f(ctx, op)
}
```

This allows users to provide either structs or simple functions.

---

## 9. SyncWorker Logic

Pseudo-code:

```go
delta := store.ClaimNext(ctx, workerID)

projection, err := projector.Project(ctx, delta.Identity)

switch {
case errors.Is(err, ErrProjectionNotFound):
    op := ProjectionOperation{
        Type:     ProjectionOpDelete,
        Identity: delta.Identity,
    }

    err = applier.Apply(ctx, op)
    if err != nil {
        store.MarkFailedOrRetry(ctx, delta, err)
        return
    }

    store.MarkSynced(ctx, delta, WithGhostDetected(true))
    return

case err != nil:
    store.MarkFailedOrRetry(ctx, delta, err)
    return

default:
    op := ProjectionOperation{
        Type:       ProjectionOpUpsert,
        Identity:   delta.Identity,
        Projection: &projection,
    }

    err = applier.Apply(ctx, op)
    if err != nil {
        store.MarkFailedOrRetry(ctx, delta, err)
        return
    }

    store.MarkSynced(ctx, delta)
    return
}
```

---

## 10. Retry Behavior

Retries should be simple.

Recommended defaults:

```text
max_attempts = 5
backoff = exponential
initial_delay = 5 seconds
max_delay = 5 minutes
```

A failed Delta should move to `retrying` if attempts remain.

A failed Delta should move to `dead` if attempts are exhausted.

```text
failed attempt + attempts remain -> retrying
failed attempt + no attempts left -> dead
```

---

## 11. Minimal YAML

YAML is optional in v0.1.

The app may wire Projector and ProjectionApplier directly in Go.

If YAML is used, keep it tiny:

```yaml
id: contacts-to-elasticsearch
mode: latest_state

projection_type: Contact

retry:
  max_attempts: 5
  backoff: exponential
  initial_delay: 5s
  max_delay: 5m
```

Do not put connector registry concepts in v0.1 YAML.

No connector IDs.

No versions.

No digests.

No dynamic plugins.

Projector and ProjectionApplier construction belongs to application code in v0.1.

Example Go wiring:

```go
projector := deltaflow.ProjectorFunc(projectContact)
applier := deltaflow.ProjectionApplierFunc(applyContactToElasticsearch)

worker := deltaflow.NewSyncWorker(store, projector, applier)
worker.Run(ctx)
```

---

## 12. Logging Policy

Do not log Projection payloads by default.

Safe log fields:

```text
delta_id
sync_id
projection_type
projection_key_hash
state
attempt_count
duration
error_code
checksum
```

Unsafe by default:

```text
projection payload
raw source rows
secrets
derived system credentials
full customer data
```

---

## 13. Design Rules

1. v0.1 supports only `latest_state`.
2. v0.1 supports one Projector and one ProjectionApplier per SyncWorker configuration.
3. Do not implement a connector registry.
4. Do not implement fan-out.
5. Do not implement envelopes or codecs.
6. Do not use CDC as the primary abstraction.
7. Do not call Deltas events.
8. Use `projection_type` and `projection_key`, not `entity_type` and `entity_id`.
9. Treat `ErrProjectionNotFound` from the Projector as a Delta Ghost.
10. Keep ProjectionAppliers idempotent in v0.1.
11. Keep the SyncWorker boring.

---


Deferred features and broader roadmap items can be documented later in [FUTURE](FUTURE.md) or [ROADMAP](ROADMAP.md).

---

## 14. Summary

DeltaFlow v0.1 is:

```text
Delta
Projector.Project()
ProjectionOperation
ProjectionApplier.Apply()
Retry
Dead
Delta Ghost
```
