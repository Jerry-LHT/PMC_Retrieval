# PMC Retrieval

面向 PMC JSON 语料的检索服务。

当前对外只提供两个 HTTP 接口：

- `POST /lookup`：按 PMCID、纯数字 PMCID、标题做快速定位
- `POST /search`：按 PubMed 风格布尔检索式查询 OpenSearch

服务设计与实现说明见 [docs/design-and-implementation.md](/Users/jerry/Documents/code/medical/pmc_retrieval/docs/design-and-implementation.md)。

## 1. `RAW_JSON_HOST_PATH=./001549 docker compose up --build -d` 启动的是什么

这条命令会启动 3 个服务：

1. `opensearch`
   - 镜像：`opensearchproject/opensearch:2.14.0`
   - 端口：`9200`
   - 作用：存储文章索引，承载 `/lookup` 和 `/search` 的实际检索
2. `postgres`
   - 镜像：`postgres:16`
   - 端口：宿主机 `5445` 映射到容器 `5432`
   - 作用：保存 `saved_queries` 表的 schema 和数据
3. `api`
   - 基于当前仓库 `Dockerfile` 构建
   - 端口：`8000`
   - 作用：运行 FastAPI，对外暴露 `/healthz`、`/lookup`、`/search`

同时，这条命令还做了两件事：

- 把宿主机的 `./001549` 挂载到容器内的 `/data/papers/raw`
- 让 `api` 容器在启动时自动执行：
  - `saved_queries` 表建表
  - OpenSearch 索引和别名检查/创建

这条命令不会自动导入 JSON 数据。

也就是说，容器起来以后，接口已经可用，但如果你还没执行导入，`/lookup` 和 `/search` 只会返回空结果。

## 2. 运行时依赖

- Docker Engine
- Docker Compose Plugin
- 可用的 PMC JSON 目录

如果你要在宿主机直接运行 Python，还需要：

- Python `>=3.11`

## 3. 配置说明

配置来源有两层：

1. YAML：`config/app.yaml`
2. 环境变量：部分字段可覆盖 YAML

关键配置：

- `paths.raw_json_dir`：原始 JSON 目录，容器内默认是 `/data/papers/raw`
- `postgres.dsn`：PostgreSQL DSN
- `opensearch.hosts`：OpenSearch 地址列表
- `opensearch.index_name`：实际索引名，默认 `articles_core_v2`
- `opensearch.index_alias`：查询与写入别名，默认 `articles_current`

已实现的环境变量覆盖：

- `PG_DSN`：覆盖 `postgres.dsn`
- `OPENSEARCH_HOSTS`：覆盖 `opensearch.hosts`
- `OS_USERNAME` / `OS_PASSWORD`：OpenSearch 鉴权

`docker-compose.yml` 里还有几项运行时变量：

- `RAW_JSON_HOST_PATH`：决定宿主机哪个目录被挂载到 `/data/papers/raw`
- `PG_USER` / `PG_PASSWORD` / `PG_DB`
- `OPENSEARCH_INITIAL_ADMIN_PASSWORD`

### 3.1 必备配置文件怎么写

最少需要 2 个配置文件：

1. `.env`
2. `config/app.yaml`

`.env` 可参考下面模板：

```env
OS_USERNAME=
OS_PASSWORD=
OPENSEARCH_HOSTS=http://localhost:9200
PG_USER=pmc
PG_PASSWORD=pmc
PG_DB=pmc_retrieval
PG_HOST=postgres
PG_PORT=5432
```

`config/app.yaml` 可参考下面模板：

```yaml
paths:
  raw_json_dir: /data/papers/raw

postgres:
  dsn: postgresql://pmc:pmc@postgres:5432/pmc_retrieval

opensearch:
  hosts:
    - http://localhost:9200
  index_name: articles_core_v2
  index_alias: articles_current
  verify_certs: false

search:
  default_page_size: 20
  max_page_size: 100
  highlight: true
  highlight_fragment_size: 120
  highlight_number_of_fragments: 2
  lookup_highlight_default: false
  pagination:
    enabled: true
    pit_keep_alive: 2m
  weights:
    title: 5
    mesh_terms: 4
    keywords: 3
    abstract_text: 2
    full_text_clean: 0

ingest:
  parse_workers: 4
  failed_log_path: logs/ingest_failed.ndjson
  progress_every: 5000
  include_full_text: false
  estimate_total_files: true
```

### 3.2 路径和端口怎么对应

`RAW_JSON_HOST_PATH=/srv/pmc-data/raw docker compose up ...` 时，路径和端口关系如下：

1. 路径：
`/srv/pmc-data/raw` (宿主机) -> `/data/papers/raw` (api 容器)
2. 端口：
`8000` (宿主机) -> `8000` (api)
`9200` (宿主机) -> `9200` (opensearch)
`5445` (宿主机) -> `5432` (postgres)

如果你在宿主机运行 Python，而不是在 `api` 容器内运行，连接地址应该是：

1. PostgreSQL: `localhost:5445`
2. OpenSearch: `localhost:9200`

## 4. 服务器部署流程

以下流程适合直接在服务器上运行整套服务。

### 4.1 准备目录

假设你准备把仓库和数据放在：

```bash
/srv/pmc-retrieval
/srv/pmc-data/raw
```

确保原始 JSON 已经放到 `/srv/pmc-data/raw`。

### 4.2 拉取代码

```bash
git clone <your-repo-url> /srv/pmc-retrieval
cd /srv/pmc-retrieval
```

### 4.3 准备环境变量

```bash
cp .env.example .env
```

如果你不需要 OpenSearch 认证，`.env` 通常只需要确认 PostgreSQL 用户名、密码、库名即可。

### 4.4 启动服务

```bash
RAW_JSON_HOST_PATH=/srv/pmc-data/raw docker compose up --build -d
```

检查容器状态：

```bash
docker compose ps
docker compose logs --tail=100 api
docker compose logs --tail=100 opensearch
docker compose logs --tail=100 postgres
```

健康检查：

```bash
curl http://127.0.0.1:8000/healthz
```

预期返回：

```json
{"status":"ok"}
```

### 4.5 首次导入数据

推荐直接在 `api` 容器里执行导入，这样不需要在服务器宿主机单独安装 Python 依赖：

```bash
docker compose exec api python -m ingest.ingest_json --raw-dir /data/papers/raw
```

大规模导入可以显式调整参数：

```bash
docker compose exec api python -m ingest.ingest_json \
  --raw-dir /data/papers/raw \
  --chunk-size 2000 \
  --max-chunk-bytes 20971520 \
  --thread-count 8 \
  --parse-workers 8 \
  --failed-log-path /app/logs/ingest_failed.ndjson \
  --progress-every 10000 \
  --optimize-index-settings
```

导入完成后，命令会输出类似：

```text
indexed=12345
```

### 4.6 重启或升级

代码更新后：

```bash
git pull
RAW_JSON_HOST_PATH=/srv/pmc-data/raw docker compose up --build -d
```

如果 OpenSearch 索引还在，不需要重复导入。

如需升级到新 mapping（例如 `articles_core_v3`），可使用迁移脚本：

```bash
docker compose exec api python -m ingest.migrate_index \
  --target-index articles_core_v3 \
  --source-index articles_current \
  --switch-alias
```

### 4.7 如何停止或关闭服务器

1. 临时停止（保留容器和数据）：

```bash
docker compose stop
```

2. 重新启动已停止的服务：

```bash
docker compose start
```

3. 完全关闭（删除容器，保留卷数据）：

```bash
docker compose down
```

4. 完全关闭并删除卷数据（危险，会删除 PostgreSQL 数据）：

```bash
docker compose down -v
```

5. 只重启 `api`：

```bash
docker compose restart api
```

6. 查看当前状态：

```bash
docker compose ps
```

## 5. 如何使用这个服务

### 5.1 先确认服务已经有数据

服务启动不代表索引里已经有文章。

第一次使用前，至少要完成：

1. `docker compose up --build -d`
2. `docker compose exec api python -m ingest.ingest_json --raw-dir /data/papers/raw`

### 5.1.1 最简 curl 示例（先用这个验证）

1. 健康检查：

```bash
curl -s 'http://127.0.0.1:8000/healthz'
```

预期：

```json
{"status":"ok"}
```

2. 最简 `lookup`（按 PMCID）：

```bash
curl -s -X POST 'http://127.0.0.1:8000/lookup' \
  -H 'Content-Type: application/json' \
  -d '{"query":"PMC11458033","page":1,"size":5,"include_fields":[]}'
```

3. 最简 `search`（布尔检索）：

```bash
curl -s -X POST 'http://127.0.0.1:8000/search' \
  -H 'Content-Type: application/json' \
  -d '{"query":"fertilization[tiab] AND China[tiab]","page":1,"size":5,"include_fields":[]}'
```

结果判断：

- 返回 JSON 中 `total > 0`：说明索引里已经有可命中数据
- 返回 JSON 中 `total = 0`：通常是导入未完成，或关键词在当前数据里无命中

### 5.2 接口总览

- `GET /healthz`：进程健康检查
- `POST /lookup`：按 PMCID 或标题查找
- `POST /search`：PubMed 风格检索

服务地址默认是：

```text
http://<server>:8000
```

### 5.3 `POST /lookup`

用途：

- 输入 `PMC11458033`
- 输入纯数字 `11458033`
- 输入完整标题或标题关键词

示例：

```bash
curl -X POST 'http://127.0.0.1:8000/lookup' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "PMC11458033",
    "page": 1,
    "size": 20,
    "include_fields": []
  }'
```

### 5.4 `POST /search`

用途：

- 用 PubMed 风格检索式查文章

示例：

```bash
curl -X POST 'http://127.0.0.1:8000/search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "(fertilization[tiab] AND China[tiab]) OR PMC11458033",
    "page": 1,
    "size": 20,
    "include_fields": ["journal", "mesh_terms_raw", "keywords"]
  }'
```

### 5.5 请求字段说明

`/lookup` 和 `/search` 的请求结构一致：

```json
{
  "query": "string",
  "page": 1,
  "size": 20,
  "cursor": null,
  "highlight": null,
  "include_fields": []
}
```

字段含义：

- `query`：查询字符串
- `page`：页码，从 `1` 开始
- `size`：每页大小，最大会被限制到 `100`
- `cursor`：游标分页令牌，传入后会使用 `search_after + PIT`
- `highlight`：是否返回高亮，`null` 时使用服务默认值（`lookup` 默认关闭）
- `include_fields`：额外返回的 `_source` 字段

默认返回字段：

- `pmcid`
- `title`
- `abstract_text`
- `publication_date`

目前可选的常用 `include_fields`：

- `journal`
- `publication_type`
- `mesh_terms_raw`
- `mesh_terms_expanded`
- `keywords`
- `full_text_clean`
- `source_json_path`

### 5.6 支持的检索语法

当前实现支持：

- `AND`
- `OR`
- `NOT`
- `()`
- `"phrase"`
- 后缀通配 `*`
- `[ti]`
- `[tiab]`
- `[ta]`
- `[mh]`
- `[mh:noexp]`
- `[pt]`
- `[dp]`
- `[pdat]`
- `[pmid]`
- PMCID 直接检索，例如 `PMC11458033`
- proximity 形式：`"heart failure"[tiab:~3]`

当前字段映射：

- `[ti]` -> `title`
- `[tiab]` -> `title`, `abstract_text`, `keywords`
- `[ta]` -> `journal`
- `[mh]` -> `mesh_terms_expanded`
- `[mh:noexp]` -> `mesh_terms_raw`
- `[pt]` -> `publication_type`
- `[dp]` / `[pdat]` -> `publication_date`
- `[pmid]` -> `pmid`

实现细节要点：

- `PMC12345` 会同时匹配 `pmcid` 和 `pmcid_numeric`
- `[dp]` / `[pdat]` 当前是精确日期匹配，不支持日期区间
- `"xxx yyy"[tiab:~3]` 当前只翻译到 `abstract_text` 的 `match_phrase + slop`
- `MeshExpander` 现在是空实现，因此 `mesh_terms_expanded` 当前等同于原始 MeSH 列表

### 5.7 返回结构

返回模型：

```json
{
  "total": 1,
  "page": 1,
  "size": 20,
  "next_cursor": "base64...",
  "hits": [
    {
      "pmcid": "PMC11458033",
      "title": "Example title",
      "abstract_text": "Example abstract",
      "publication_date": "2024-10-07",
      "score": 12.34,
      "highlight": {
        "title": ["<em>Example</em> title"]
      },
      "source": {
        "pmcid": "PMC11458033",
        "title": "Example title"
      }
    }
  ]
}
```

## 6. 数据导入规则

导入入口：

```bash
python -m ingest.ingest_json --raw-dir /data/papers/raw
```

导入器行为：

- 递归扫描 `*.json`
- 忽略 `*.txt`、`*.jpg` 等非 JSON 文件
- 跳过非法 JSON 和非对象结构
- 从原始字段构建标准化文档
- 用 `doc_id=pmcid` 写入 OpenSearch
- 同一个 PMCID 重复出现时，最后一次写入会覆盖前面的内容
- 支持 `--parse-workers` 并行解析，写入端单通道 bulk
- 失败记录输出到 `ndjson`（可用 `--retry-failed-from` 重放）
- 每 `N` 条输出进度（吞吐、失败计数、队列深度、ETA）

日期标准化规则：

- `9` 位或 `10` 位 epoch -> `YYYY-MM-DD`
- `YYYY-MM-DD` / `YYYY/MM/DD` / `YYYY MM DD` -> 标准日期
- `YYYY-MM` -> 补成该月 `01`
- `YYYY` -> 补成该年 `01-01`
- 非法日期、空字符串、`None` -> `null`

### 6.1 小数据快速验证（建议先跑这个）

大数据导入前，建议先做一次 5 个文件以内的小样本验证。

示例流程：

```bash
mkdir -p /tmp/pmc-mini
find ./001549 -type f -name '*.json' | head -n 5 | xargs -I{} cp "{}" /tmp/pmc-mini/
RAW_JSON_HOST_PATH=/tmp/pmc-mini docker compose up --build -d
docker compose exec api python -m ingest.ingest_json --raw-dir /data/papers/raw --thread-count 2 --chunk-size 100
curl -X POST 'http://127.0.0.1:8000/search' \
  -H 'Content-Type: application/json' \
  -d '{"query":"PMC","page":1,"size":5,"include_fields":[]}'
```

如果返回 `total > 0`，说明整条链路（挂载目录 -> 导入 -> 检索）是通的。

### 6.2 `raw` 目录是否支持多文件

支持，且是递归处理。

实现位于 [ingest/ingest_json.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/ingest_json.py#L11)：

1. 使用 `raw_dir.rglob("*.json")` 递归扫描目录树
2. 会处理该目录及其所有子目录下的全部 `.json` 文件
3. 非 `.json` 文件会被忽略

## 7. 本机开发运行

如果你想在宿主机直接调试 Python 代码，推荐只用 Docker 起依赖：

```bash
docker compose up -d opensearch postgres
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

宿主机运行 API 时，至少设置：

```bash
export PG_DSN='postgresql://pmc:pmc@localhost:5445/pmc_retrieval'
export OPENSEARCH_HOSTS='http://localhost:9200'
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

宿主机运行导入器时，也建议显式覆盖 OpenSearch 地址：

```bash
OPENSEARCH_HOSTS='http://localhost:9200' ./.venv/bin/python -m ingest.ingest_json --raw-dir ./001549
```

## 8. 测试

安装开发依赖：

```bash
pip install -e '.[dev]'
```

运行测试：

```bash
pytest
```

当前测试主要覆盖：

- 日期解析
- JSON 导入构建
- PubMed 语法解析与翻译

## 9. 运行注意事项

1. `docker compose up --build -d` 不会自动导入数据。
2. `api` 启动依赖 `postgres` 和 `opensearch` 都可连接；任一不可用，API 进程启动会失败。
3. 当前 `docker-compose.yml` 只给 PostgreSQL 挂了持久卷，没有给 OpenSearch 挂持久卷。
   - 这意味着如果 OpenSearch 容器被删除或重建，索引数据可能丢失，需要重新导入。
4. 当前 `docker-compose.yml` 对宿主机暴露了 `8000`、`9200`、`5445`。
   - 如果部署在公网服务器上，建议配合防火墙、反向代理或内网访问控制。
5. `saved_queries` 表已经实现仓储层，但目前没有对外 API 使用这张表。

## 10. 常见问题

### 10.1 `curl /healthz` 正常，但查询没有结果

通常原因是还没导入数据。

执行：

```bash
docker compose exec api python -m ingest.ingest_json --raw-dir /data/papers/raw
```

### 10.2 `Failed to resolve 'opensearch'`

通常是你在宿主机直接运行 Python，但配置里仍然使用 Docker 网络内的主机名。

执行：

```bash
export OPENSEARCH_HOSTS='http://localhost:9200'
```

### 10.3 `Connection refused` 到 `localhost:9200`

说明 OpenSearch 还没启动好，或者启动失败。

排查：

```bash
docker compose ps
docker compose logs --tail=100 opensearch
curl http://127.0.0.1:9200
```

### 10.4 OpenSearch 容器启动后立即退出

OpenSearch 2.12+ 要求设置初始化管理员密码。

本项目已经在 `docker-compose.yml` 里给了默认值；如果需要改，先设置：

```bash
export OPENSEARCH_INITIAL_ADMIN_PASSWORD='YourStrongPassword!2026'
```
