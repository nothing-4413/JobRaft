# internal/api

HTTP 层。它把调度器以及（可选）集群注册表映射为 JSON 端点，并加入鉴权。

## 端点

任务、Worker 与运维路由见 [`docs/api.md`](../../docs/api.md)。简言之：

- `POST /tasks`、`GET /tasks`、`GET /tasks/{id}`、`DELETE /tasks/{id}`、`DELETE /tasks`、`POST /tasks/{id}?complete=true`
- `POST /workers`、`POST /workers/{id}`、`POST /workers/{id}/claim`、`POST /workers/{id}/tasks/renew`、`GET /workers`
- `GET /healthz`、`GET /readyz`、`GET /metrics`、`GET /cluster`、`GET /admin`

## 鉴权

用 `NewWithToken`（或 `NewWithClusterToken`）构造服务器。设置 token 后，除 `/healthz` 与 `/readyz` 外的所有路由都要求 `Authorization: Bearer <token>` 或 `X-API-Key: <token>`。空 token 会关闭鉴权，仅用于本地开发。`/admin` 是一个小的自刷新 HTML 看板。
