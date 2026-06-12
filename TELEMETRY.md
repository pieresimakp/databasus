# Databasus Telemetry — Integration Manual

Anonymous, opt-in usage telemetry sent from a running Databasus instance to the central analytics service. Each instance pings periodically with its app version, OS/arch, instance user count, and the set of database / storage / notifier types it has configured. We use this to understand which platforms and integrations are actually in use so we can prioritize work.

No personal data, no DB contents, no user identifiers. The only stable identifier is a per-instance UUID generated locally on first run.

---

## Endpoint

```
POST https://metrics.databasus.com/api/anonymous/collect
```

- **Method:** `POST`
- **Content-Type:** `application/json`
- **Auth:** none (anonymous)
- **TLS:** required (HTTPS only; HTTP is redirected)
- **Recommended User-Agent:** `Databasus-Telemetry/<app-version>`.

---

## Request body

```json
{
  "instanceID": "550e8400-e29b-41d4-a716-446655440000",
  "appVersion": "1.5.0",
  "os": "linux",
  "arch": "amd64",
  "installedAt": "2026-01-15",
  "userCount": 7,
  "databases": [
    {
      "type": "POSTGRES_LOGICAL", "version": "16", "rawSizeMb": 4321, "backupSizeMb": 870,
      "verification": { "isEnabled": true, "scheduleType": "INTERVAL", "intervalType": "DAILY" }
    },
    {
      "type": "POSTGRES_PHYSICAL", "version": "17", "backupType": "FULL_INCREMENTAL_WAL_STREAM",
      "rawSizeMb": 192000, "backupSizeMb": 38400,
      "verification": { "isEnabled": true, "scheduleType": "AFTER_BACKUP" }
    },
    { "type": "MYSQL", "version": "8.0" }
  ],
  "storages": ["LOCAL", "S3", "S3"],
  "notifiers": ["TELEGRAM", "EMAIL"],
  "verificationAgents": [
    { "maxCpu": 4, "maxRamGb": 8,  "maxDiskGb": 100, "maxConcurrentJobs": 2 },
    { "maxCpu": 8, "maxRamGb": 16, "maxDiskGb": 200, "maxConcurrentJobs": 4 }
  ]
}
```

### Field reference

| Field                       | Type                       | Required               | Constraints                                                          | Notes                                                                                                                          |
| --------------------------- | -------------------------- | ---------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `instanceID`                | string (UUID v4)           | **yes**                | must parse as a valid UUID                                           | Generate **once** on first launch and persist locally (e.g. config file). Reuse forever — same UUID every ping.                |
| `appVersion`                | string                     | **yes**                | 1–64 chars                                                           | Semver-ish, e.g. `1.4.2`, `2.0.0-beta3`.                                                                                       |
| `os`                        | string                     | **yes**                | 1–64 chars                                                           | Use `runtime.GOOS` values: `linux`, `darwin`, `windows`.                                                                       |
| `arch`                      | string                     | **yes**                | 1–64 chars                                                           | Use `runtime.GOARCH` values: `amd64`, `arm64`, `386`.                                                                          |
| `installedAt`               | string                     | optional               | format `YYYY-MM-DD`, 1–64 chars                                      | Date the instance was first installed. Omit or send `""` if unknown. Invalid dates are silently ignored.                       |
| `userCount`                 | integer                    | optional               | `0 ≤ value`                                                          | Added 2026-06-12. Total user accounts in the instance. Omit (or `0`) if unknown; old clients that don't send it count as unknown. |
| `databases`                 | array of database entry    | **yes** (may be empty) | ≤ 200 entries                                                        | One entry per real database. Duplicates are NOT deduped — send one entry per database, even if two share `type` and `version`. |
| `databases[].type`          | string enum                | **yes**                | 1–64 chars                                                           | Enum: `POSTGRES_LOGICAL`, `POSTGRES_PHYSICAL`, `MYSQL`, `MARIADB`, `MONGODB`. **`POSTGRES_LOGICAL` is the engine formerly reported as `POSTGRES` — treat the two as one in aggregation.** |
| `databases[].version`       | string                     | **yes**                | 1–64 chars                                                           | E.g. `16`, `8.0`. For `POSTGRES_PHYSICAL`, `17` or `18` (physical backups require PG 17/18).                                   |
| `databases[].backupType`    | string enum                | required when `type="POSTGRES_PHYSICAL"`; forbidden otherwise | enum: `FULL`, `FULL_INCREMENTAL`, `FULL_INCREMENTAL_WAL_STREAM`      | Added 2026-06-12. Physical backup strategy: FULL-only vs FULL+incremental vs FULL+incremental+WAL stream. Lets physical adoption be split by strategy. Omitted for all non-physical types. |
| `databases[].rawSizeMb`     | integer                    | optional               | `0 ≤ value ≤ 1_073_741_824` (≈ 1 PiB); 1 MB = 1024 × 1024 bytes      | Database on-disk size in MB. Omit if unknown. For `POSTGRES_PHYSICAL` this is taken from the latest completed physical **FULL** backup; for all other types, from the latest completed logical backup. |
| `databases[].backupSizeMb`  | integer                    | optional               | `0 ≤ value ≤ 1_073_741_824` (≈ 1 PiB); 1 MB = 1024 × 1024 bytes      | Compressed backup size in MB. Omit if unknown. Same source as `rawSizeMb`: physical FULL backup for `POSTGRES_PHYSICAL`, latest logical backup otherwise. |
| `databases[].verification`             | object                | optional               | omit when verification is not configured for this database           | Added 2026-05-20. Absence means "feature not configured for this DB"; equivalent to `isEnabled: false` from a coverage point of view. |
| `databases[].verification.isEnabled`   | boolean               | **yes** (within object) | —                                                                  | `true` iff **scheduled** verification is enabled. Manual one-off runs are not represented here.                                |
| `databases[].verification.scheduleType`| string enum           | **yes** (within object) | enum: `AFTER_BACKUP`, `INTERVAL`                                   | `AFTER_BACKUP` = run after each successful backup. `INTERVAL` = run on a recurring schedule. Unknown values → `400`.            |
| `databases[].verification.intervalType`| string enum           | required when `scheduleType="INTERVAL"`; forbidden otherwise | enum: `HOURLY`, `DAILY`, `WEEKLY`, `MONTHLY`, `CRON`                | Cadence bucket only. Time-of-day, weekday, day-of-month, and raw cron expressions are **deliberately not transmitted**.          |
| `storages`                  | array of string            | **yes** (may be empty) | ≤ 200 entries; each 1–64 chars                                       | E.g. `LOCAL`, `S3`, `GCS`, `AZURE_BLOB`. One entry per configured storage destination — duplicates are NOT deduped.            |
| `notifiers`                 | array of string            | **yes** (may be empty) | ≤ 200 entries; each 1–64 chars                                       | E.g. `EMAIL`, `TELEGRAM`, `SLACK`, `WEBHOOK`. One entry per configured notifier — duplicates are NOT deduped.                  |
| `verificationAgents`        | array of object            | recommended (may be empty); old clients may omit (treated as empty) | ≤ 200 entries                                            | Added 2026-05-20. One entry per registered, not-soft-deleted verification agent. Identifiers (UUIDs, names, hostnames, IPs) are intentionally omitted — capacity only. |
| `verificationAgents[].maxCpu`            | integer              | **yes**                | `0 ≤ value ≤ 4096`                                                  | Cores the agent declared. `0` means the agent has never reported capacity (no heartbeat yet).                                  |
| `verificationAgents[].maxRamGb`          | integer              | **yes**                | `0 ≤ value ≤ 4_194_304` (≈ 4 PiB)                                   | GB; `0` if not yet reported.                                                                                                   |
| `verificationAgents[].maxDiskGb`         | integer              | **yes**                | `0 ≤ value ≤ 16_777_216` (≈ 16 PiB)                                 | GB; `0` if not yet reported.                                                                                                   |
| `verificationAgents[].maxConcurrentJobs` | integer              | **yes**                | `0 ≤ value ≤ 1024`                                                  | Parallelism cap; `0` if not yet reported.                                                                                      |

**Limits at a glance:**

- All scalar string fields: **64 characters max** (UTF-8 runes, not bytes).
- All arrays: **200 entries max**.
- Whole request body: **64 KB max**.

**Back-compat for verification fields:** `verificationAgents` may be omitted by old clients; absence is accepted and treated as empty (`[]`). `databases[].verification` may be omitted on any database; absence means "verification not configured for that database". Both fields are purely additive — old payloads continue to return `202`.

**Back-compat for the database-type split:** the old `POSTGRES` type was split into `POSTGRES_LOGICAL` and `POSTGRES_PHYSICAL`. Current clients emit only the new strings, but historical payloads (and aggregated history) may still carry the legacy `POSTGRES`; treat `POSTGRES` and `POSTGRES_LOGICAL` as the same engine when aggregating. `databases[].backupType` (physical-only) and the instance-level `userCount` are purely additive — absence on older clients is accepted and old payloads continue to return `202`.

---

## Responses

| Status                  | Body                    | Meaning                                                                                                                                                                                                       |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `202 Accepted`          | `{"status":"accepted"}` | Event accepted.                                                                                                                                                                                               |
| `400 Bad Request`       | `{"error":"<reason>"}`  | Validation failed. `<reason>` is human-readable, e.g. `"instanceID must be a valid UUID"`, `"appVersion exceeds maximum length of 64 characters"`, `"databases array exceeds maximum length"`. Fix and retry. |
| `413 Payload Too Large` | varies                  | Request body exceeds the 64 KB cap. Trim arrays.                                                                                                                                                              |
| `4xx` / `5xx`           | varies                  | Treat as transient. Do not retry within the same scheduled tick.                                                                                                                                              |

The response is informational — don't gate any user-visible behavior on it. If the request fails, log and move on.

---

## Cadence

Send at most **one ping per 24 hours** per instance. Higher frequency provides no additional value and excess pings will not be counted. Recommended pattern:

- One ping at app startup, after the first 60 seconds of healthy operation.
- One ping every 24 hours afterward, jittered by ±10 minutes to avoid thundering-herd at midnight UTC.

---

## Recommended client behavior

1. **Generate `instanceID` once.** UUID v4. Persist to your config dir (`~/.databasus/instance-id` or wherever your config lives). Never regenerate.
2. **Make telemetry opt-in or opt-out per your privacy policy** and respect it.
3. **Set the User-Agent** to `Databasus-Telemetry/<your-version>`.
4. **Use a 5-second timeout.** Telemetry must never block or slow down the app's real work.
5. **Fire-and-forget.** Do not retry on failure — by the next scheduled ping the data point is stale anyway. (Network blip → just skip this round.)
6. **Send the full snapshot every time**, not deltas. Each ping replaces the previous "currently-active" view of the instance.
7. **Cap your own arrays before sending.** Truncate to ≤ 200 entries; if you have more, you have a bug elsewhere.

---

## Example: Go client

```go
package telemetry

import (
    "bytes"
    "context"
    "encoding/json"
    "net/http"
    "time"
)

type DatabaseVerificationEntry struct {
    IsEnabled    bool   `json:"isEnabled"`
    ScheduleType string `json:"scheduleType"`
    IntervalType string `json:"intervalType,omitempty"`
}

type VerificationAgentEntry struct {
    MaxCpu            int `json:"maxCpu"`
    MaxRamGb          int `json:"maxRamGb"`
    MaxDiskGb         int `json:"maxDiskGb"`
    MaxConcurrentJobs int `json:"maxConcurrentJobs"`
}

type DatabaseEntry struct {
    Type         string                     `json:"type"`
    Version      string                     `json:"version"`
    BackupType   string                     `json:"backupType,omitempty"` // POSTGRES_PHYSICAL only
    RawSizeMb    *int64                     `json:"rawSizeMb,omitempty"`
    BackupSizeMb *int64                     `json:"backupSizeMb,omitempty"`
    Verification *DatabaseVerificationEntry `json:"verification,omitempty"`
}

type CollectRequest struct {
    InstanceID         string                   `json:"instanceID"`
    AppVersion         string                   `json:"appVersion"`
    OS                 string                   `json:"os"`
    Arch               string                   `json:"arch"`
    InstalledAt        string                   `json:"installedAt,omitempty"`
    UserCount          int                      `json:"userCount,omitempty"`
    Databases          []DatabaseEntry          `json:"databases"`
    Storages           []string                 `json:"storages"`
    Notifiers          []string                 `json:"notifiers"`
    VerificationAgents []VerificationAgentEntry `json:"verificationAgents,omitempty"`
}

func Send(ctx context.Context, req *CollectRequest, version string) error {
    body, err := json.Marshal(req)
    if err != nil {
        return err
    }

    httpReq, err := http.NewRequestWithContext(
        ctx, http.MethodPost,
        "https://metrics.databasus.com/api/anonymous/collect",
        bytes.NewReader(body),
    )
    if err != nil {
        return err
    }
    httpReq.Header.Set("Content-Type", "application/json")
    httpReq.Header.Set("User-Agent", "Databasus-Telemetry/"+version)

    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(httpReq)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil // fire-and-forget; ignore status
}
```

---

## Example: curl

```bash
curl -sS -X POST https://metrics.databasus.com/api/anonymous/collect \
  -H "Content-Type: application/json" \
  -H "User-Agent: Databasus-Telemetry/1.5.0" \
  -d '{
    "instanceID": "550e8400-e29b-41d4-a716-446655440000",
    "appVersion": "1.5.0",
    "os":         "linux",
    "arch":       "amd64",
    "installedAt":"2026-01-15",
    "userCount": 7,
    "databases":  [
      {"type":"POSTGRES_LOGICAL","version":"16","rawSizeMb":4321,"backupSizeMb":870,
       "verification":{"isEnabled":true,"scheduleType":"INTERVAL","intervalType":"DAILY"}},
      {"type":"POSTGRES_PHYSICAL","version":"17","backupType":"FULL_INCREMENTAL_WAL_STREAM","rawSizeMb":192000,"backupSizeMb":38400,
       "verification":{"isEnabled":true,"scheduleType":"AFTER_BACKUP"}}
    ],
    "storages":   ["LOCAL","S3"],
    "notifiers":  ["TELEGRAM"],
    "verificationAgents": [
      {"maxCpu":4,"maxRamGb":8,"maxDiskGb":100,"maxConcurrentJobs":2},
      {"maxCpu":8,"maxRamGb":16,"maxDiskGb":200,"maxConcurrentJobs":4}
    ]
  }'
```

Expected: `HTTP/1.1 202 Accepted` and `{"status":"accepted"}`.

---

## Public report

The aggregated overview — counts and distributions only, never per-instance data — is published at `GET https://metrics.databasus.com/api/reports/overview`.
