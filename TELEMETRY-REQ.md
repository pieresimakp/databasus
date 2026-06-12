# Telemetry compatibility — what we keep and why

Telemetry pings come from Databasus instances installed in the wild. A user installs once, may never update, and pings forever. The server's contract with old clients is therefore **append-only**: every field that has ever been valid stays valid, every behavior an old client relied on stays observable. We never reject a payload that worked in a prior version.

## Always-accepted optional fields

- `installedAt` — always optional. Invalid date strings are silently ignored, never `400`.
- `databases[].rawSizeMb`, `databases[].backupSizeMb` — added 2026-05-05. Optional from the start; NULL in DB; rendered as the "unknown" bucket on the dashboard. Old clients that omit them keep working unchanged.
- `databases[].verification`, `verificationAgents` — added 2026-05-20. Both optional from the start; verification columns nullable in `ping_databases`; `ping_verification_agents` empty when no agents reported. Old clients that omit them keep working unchanged.

## Headers and response contract

- `User-Agent` is recommended (`Databasus-Telemetry/<version>`) but not required. The legacy prefix `Postgresus-Telemetry/<version>` is still accepted silently for instances that pre-date the rename. Any other UA (or missing UA) logs a warning, never rejects.
- `202 Accepted` is returned for accepted pings AND for silently rate-limited/abuse-suppressed pings — clients can't distinguish the two and shouldn't try to. `400` is only for malformed payloads (bad UUID, oversize fields, oversize arrays, body > 64 KB, negative/over-cap sizes).

## Behavior changes — what stayed compatible

- **2026-05-05 — server-side dedup removed** for `databases`, `storages`, `notifiers`. Each entry now represents one real configured item (a single instance can run `2× POSTGRES 17`, `2× S3`, `2× SLACK`). Wire format unchanged; payloads that worked before still return `202`. Old clients that happened to send duplicates will now have those duplicates persisted instead of collapsed — dashboard counts grow accordingly, but no `4xx`.
- **2026-05-05 — User-Agent prefix renamed** from `Postgresus-Telemetry/...` to `Databasus-Telemetry/...`. The old prefix is still recognized and does NOT log a warning — old instances ping with the legacy prefix forever and we treat it as expected.

## Rules when changing this surface

1. New fields: optional, `omitempty` on the wire, nullable column. Never required.
2. Validation: only reject what was already invalid. Never tighten an existing constraint.
3. Removed fields: keep accepting them in the JSON parser (ignore in the service). Never `400` an old client for sending a field we no longer use.
4. New endpoints / paths are fine. Don't rename or delete existing ones.
