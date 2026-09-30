# deploy

Deployment assets for the reference multi-instance stack.

- `prometheus.yml` — scrape config targeting both API instances' `/metrics` with
  the local bearer token.
- `grafana/provisioning/datasources/prometheus.yml` — pre-provisioned Prometheus
  datasource.
- `grafana/provisioning/dashboards/dashboards.yml` — file-based dashboard
  provider.
- `grafana/dashboards/jobraft-overview.json` — the "JobRaft Overview" dashboard.

The stack itself is defined in the repository-root
[`docker-compose.yml`](../docker-compose.yml). Run `docker compose up --build`
and open Grafana on `:3000` (`admin` / `local-dev-password`) and Prometheus on
`:9090`. See [`docs/operations.md`](../docs/operations.md) for operational
assumptions and the verification checklist.
