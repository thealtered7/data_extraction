# Metrics

Meter names and span names use the prefix `extract_data`. The OTLP resource attribute `service.name` is `extract_data`.

`postgres-meta` records these meters on the `ObservabilityFactory` registry. On shutdown the process publishes metrics to `MANAGEMENT_OTLP_METRICS_EXPORT_URL` and flushes spans to `MANAGEMENT_OTLP_TRACING_ENDPOINT`. Both default to `http://localhost:4318`. `make postgres-meta-docker` points them at `otel-collector:4318`.

## Postgres metadata

Recorded by `PostgresMetaDataDB` through `Observability.observeCallable`. Each call increments a counter and records an observation timer.

| Meter | Type | When | Tags |
| --- | --- | --- | --- |
| `extract_data.postgres_metadata.get_high_water_mark.records` | counter | `getHighWaterMark()` | `outcome` = `success` or `error` |
| `extract_data.postgres_metadata.get_high_water_mark` | timer | same call | `instance_name`, `outcome`, `error` |
| `extract_data.postgres_metadata.get_high_water_mark.active` | long task timer | while the call is in progress | `instance_name` |
| `extract_data.postgres_metadata.get_replication_slot_status.records` | counter | `getReplicationSlotStatus()` | `outcome` = `success` or `error` |
| `extract_data.postgres_metadata.get_replication_slot_status` | timer | same call | `instance_name`, `slot_name`, `outcome`, `error` |
| `extract_data.postgres_metadata.get_replication_slot_status.active` | long task timer | while the call is in progress | `instance_name`, `slot_name` |

`error` is `none` on success. On failure it is the exception class name.

## JVM and process

Bound when the application starts `ObservabilityFactory` and exported with the same resource attribute `service.name=extract_data`.

| Meter | Type | Description |
| --- | --- | --- |
| `jvm.classes.loaded` | gauge | Classes currently loaded |
| `jvm.classes.unloaded` | counter | Classes unloaded |
| `jvm.memory.used` | gauge | Used memory |
| `jvm.memory.committed` | gauge | Committed memory |
| `jvm.memory.max` | gauge | Max memory |
| `jvm.buffer.count` | gauge | Direct and mapped buffer count |
| `jvm.buffer.memory.used` | gauge | Buffer memory used |
| `jvm.buffer.total.capacity` | gauge | Buffer capacity |
| `jvm.threads.live` | gauge | Live threads |
| `jvm.threads.daemon` | gauge | Daemon threads |
| `jvm.threads.peak` | gauge | Peak live threads |
| `jvm.threads.started` | counter | Threads started |
| `jvm.threads.states` | gauge | Threads by state |
| `jvm.gc.memory.allocated` | counter | Allocated heap between GC |
| `jvm.gc.memory.promoted` | counter | Memory promoted to old generation |
| `jvm.gc.max.data.size` | gauge | Max old-generation size |
| `jvm.gc.live.data.size` | gauge | Old-generation size after full GC |
| `jvm.gc.pause` | timer | GC pause time |
| `jvm.gc.concurrent.phase.time` | timer | Concurrent GC phase time |
| `system.cpu.count` | gauge | Processors available to the JVM |
| `system.cpu.usage` | gauge | Recent system CPU usage |
| `system.load.average.1m` | gauge | One-minute system load average |
| `process.cpu.usage` | gauge | Recent JVM process CPU usage |
| `process.cpu.time` | counter | JVM process CPU time, in nanoseconds |

## Named but not recorded

These constants exist on `Observability` and no call site increments them:

- `extract_data.file_writes`
- `extract_data.flush_notification`
- `extract_data.cdc_event_written_to_buffer`
- `extract_data.cdc_file_written`
