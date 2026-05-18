# SSL test certificates

Self-signed cert + key shared by every SSL test container (Postgres, MariaDB, MySQL, MongoDB).
Mounted into the SSL containers defined in `docker-compose.yml`. Not used in production.

The cert validates against `CN=localhost`, but every backup/restore code path uses
`InsecureSkipVerify`/`--skip-ssl-verify-server-cert`, so any self-signed cert works.

To regenerate (run from this directory):

```
openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt -days 3650 -nodes -subj "/CN=localhost"
cat server.crt server.key > server.pem
```

- `server.crt` / `server.key` — used by Postgres
- `server.pem` — combined cert+key, used by MongoDB
- `pg_hba.conf` — SSL-only auth rules for the Postgres SSL container; rejects every plaintext TCP connection so a silent SSL-drop regression fails the test
- MariaDB and MySQL containers ignore these files; they auto-generate their own self-signed cert on first start, and `--require_secure_transport=ON` rejects non-TLS clients
