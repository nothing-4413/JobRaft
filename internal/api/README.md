# internal/api

The HTTP layer. It maps the scheduler and, optionally, the cluster registry onto
JSON endpoints and adds authentication.

## Endpoints

Task, worker, and operations routes are documented in
[`docs/api.md`](../../docs/api.md). In short:

- `POST /tasks`, `GET /tasks`, `GET /tasks/{id}`, `DELETE /tasks/{id}`,
  `DELETE /tasks`, `POST /tasks/{id}?complete=true`
- `POST /workers`, `POST /workers/{id}`, `POST /workers/{id}/claim`,
  `POST /workers/{id}/tasks/renew`, `GET /workers`
- `GET /healthz`, `GET /readyz`, `GET /metrics`, `GET /cluster`, `GET /admin`

## Authentication

Construct the server with `NewWithToken` (or `NewWithClusterToken`). When a
token is set, every route except `/healthz` and `/readyz` requires
`Authorization: Bearer <token>` or `X-API-Key: <token>`. An empty token disables
authentication for local development. `/admin` is a small self-refreshing HTML
dashboard.
