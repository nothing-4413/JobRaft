# deploy

参考多实例栈的部署资产。

- `prometheus.yml` —— 抓取配置，用本地 bearer token 采集两个 API 实例的 `/metrics`。
- `grafana/provisioning/datasources/prometheus.yml` —— 预置的 Prometheus 数据源。
- `grafana/provisioning/dashboards/dashboards.yml` —— 基于文件的看板 provider。
- `grafana/dashboards/jobraft-overview.json` —— "JobRaft Overview" 看板。

栈本体定义在仓库根目录的 [`docker-compose.yml`](../docker-compose.yml)。运行 `docker compose up --build`，然后在 `:3000`（`admin` / `local-dev-password`）打开 Grafana，在 `:9090` 打开 Prometheus。运维假设与验证清单见 [`docs/operations.md`](../docs/operations.md)。
