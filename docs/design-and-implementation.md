# PMC Retrieval 设计与实现

本文面向后续开发者，说明当前代码库的运行结构、核心数据流、模块职责和已知限制。

## 1. 项目目标

当前实现范围很明确：

1. 提供一个面向 PMC JSON 语料的检索 API
2. 支持两类查询：
   - `/lookup`：PMCID / 标题定位
   - `/search`：PubMed 风格布尔检索

这不是一个“抓取 + 清洗 + 检索 + 管理后台”的全量平台。

当前代码库主要覆盖：

- JSON 文档标准化
- OpenSearch 索引写入
- PubMed 查询语法解析与翻译
- FastAPI 查询接口
- PostgreSQL 中 `saved_queries` 表的仓储层

## 2. 运行时架构

默认通过 `docker-compose.yml` 启动 3 个进程：

1. `opensearch`
   - 检索引擎
   - 保存文章索引
2. `postgres`
   - 保存 `saved_queries`
3. `api`
   - FastAPI HTTP 服务
   - 负责 API 请求、启动时初始化、索引访问

运行关系：

- `api` 依赖 `postgres`
- `api` 依赖 `opensearch`
- 原始 JSON 不进入 PostgreSQL，而是由导入器直接写入 OpenSearch

## 3. 启动时序

入口在 [app/main.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/main.py)。

FastAPI `lifespan` 启动阶段会执行：

1. 读取配置
2. 构造依赖容器 `AppContainer`
3. 调用 `SavedQueryRepository.ensure_schema()`
4. 调用 `OpenSearchGateway.ensure_index()`

这意味着：

- PostgreSQL 连接不上时，API 启动失败
- OpenSearch 连接不上时，API 启动失败
- 索引不存在时会自动创建
- 别名不存在时会自动创建

不会自动执行的事情：

- 不会自动扫描 `/data/papers/raw`
- 不会自动导入 JSON
- 不会自动做增量同步

## 4. 代码模块划分

### 4.1 `app/`

- [app/main.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/main.py)
  - FastAPI 应用入口
  - 注册路由和启动生命周期
- [app/config.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/config.py)
  - 读取 YAML 和环境变量
  - 生成 `Settings`
- [app/dependencies.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/dependencies.py)
  - 组装 `OpenSearchGateway`、`LookupService`、`SearchService`、`SavedQueryRepository`
- [app/api_lookup.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/api_lookup.py)
  - `/lookup`
- [app/api_search.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/api_search.py)
  - `/search`
- [app/models.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/models.py)
  - 请求和响应模型

### 4.2 `ingest/`

- [ingest/ingest_json.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/ingest_json.py)
  - 导入 CLI
  - 递归扫描 JSON
  - 控制 bulk 参数
- [ingest/document_builder.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/document_builder.py)
  - 从原始 PMC JSON 抽取标准化字段
- [ingest/date_utils.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/date_utils.py)
  - 日期归一化

### 4.3 `parser/`

- [parser/grammar.lark](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/grammar.lark)
  - PubMed 风格语法定义
- [parser/pubmed_parser.py](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/pubmed_parser.py)
  - 语法解析和 AST 生成
- [parser/ast_nodes.py](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/ast_nodes.py)
  - AST 节点
- [parser/translator.py](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/translator.py)
  - AST -> OpenSearch DSL

### 4.4 `search/`

- [search/opensearch_client.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/opensearch_client.py)
  - OpenSearch 客户端封装
  - 索引初始化
  - bulk upsert
  - 查询执行
- [search/lookup_service.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/lookup_service.py)
  - `/lookup` 业务逻辑
- [search/search_service.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/search_service.py)
  - `/search` 业务逻辑
- [search/mapping.json](/Users/jerry/Documents/code/medical/pmc_retrieval/search/mapping.json)
  - 索引 mapping

### 4.5 `storage/`

- [storage/postgres.py](/Users/jerry/Documents/code/medical/pmc_retrieval/storage/postgres.py)
  - PostgreSQL 连接上下文
- [storage/saved_queries.py](/Users/jerry/Documents/code/medical/pmc_retrieval/storage/saved_queries.py)
  - `saved_queries` 仓储

## 5. 数据流

### 5.1 导入链路

数据流如下：

```text
宿主机原始 JSON 目录
  -> ingest.ingest_json.iter_documents()
  -> ingest.document_builder.build_document()
  -> search.opensearch_client.OpenSearchGateway.bulk_upsert_iter()
  -> OpenSearch alias: articles_current
```

导入器直接从原始 JSON 构造最终检索文档。

当前没有“标准化 JSON 落盘层”，也没有中间数据库表。

### 5.2 查询链路

`/lookup`：

```text
HTTP 请求
  -> app/api_lookup.py
  -> search/lookup_service.py
  -> OpenSearchGateway.search()
  -> OpenSearch
```

`/search`：

```text
HTTP 请求
  -> app/api_search.py
  -> parser/pubmed_parser.py
  -> parser/translator.py
  -> search/search_service.py
  -> OpenSearchGateway.search()
  -> OpenSearch
```

## 6. 索引文档模型

标准化文档由 [ingest/document_builder.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/document_builder.py) 生成，字段包括：

- `doc_id`
- `pmcid`
- `pmcid_numeric`
- `pmid`
- `title`
- `title_normalized`
- `journal`
- `publication_date`
- `publication_type`
- `abstract_text`
- `mesh_terms_raw`
- `mesh_terms_expanded`
- `keywords`
- `full_text_clean`
- `source_json_path`

几个关键点：

1. `doc_id` 直接等于大写 `pmcid`
2. `pmcid_numeric` 通过正则从 `PMC12345` 提取
3. `pmid` 当前固定写 `None`
4. `mesh_terms_expanded` 当前没有真正展开逻辑
5. `source_json_path` 记录原始文件路径，便于回查

索引 mapping 定义在 [search/mapping.json](/Users/jerry/Documents/code/medical/pmc_retrieval/search/mapping.json)。

`dynamic` 被关闭，因此新增字段不会自动进入索引。新增字段时必须同步修改：

1. `search/mapping.json`
2. `ingest/document_builder.py`
3. 可能涉及的查询翻译逻辑
4. 文档和测试

## 7. 查询设计

### 7.1 `/lookup`

[search/lookup_service.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/lookup_service.py) 会根据输入类型拼接 `should` 查询：

- `PMC\d+`：高权重匹配 `pmcid` 与 `pmcid_numeric`
- 纯数字：反向补出 `PMC{n}` 再匹配
- 标题：
  - `title.keyword`
  - `title_normalized`
  - `match_phrase(title)`
  - `match(title)`

这个接口的定位是“快速命中”，不是完整的语义搜索。

### 7.2 `/search`

[search/search_service.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/search_service.py) 做三件事：

1. 用 Lark 解析 PubMed 风格表达式
2. 翻译为 OpenSearch DSL
3. 执行查询并整理响应

当前支持的语法：

- `AND`
- `OR`
- `NOT`
- 括号
- 短语
- 后缀 `*`
- `[ti]`
- `[tiab]`
- `[ta]`
- `[mh]`
- `[mh:noexp]`
- `[pt]`
- `[dp]`
- `[pdat]`
- `[pmid]`
- PMCID 直接写入查询
- `"phrase"[tiab:~N]`

### 7.3 翻译规则

主要翻译规则见 [parser/translator.py](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/translator.py)：

- `[ti]` -> `title`
- `[tiab]` -> `title`, `abstract_text`, `keywords`
- `[ta]` -> `journal`
- `[mh]` -> `mesh_terms_expanded`
- `[mh:noexp]` -> `mesh_terms_raw`
- `[pt]` -> `publication_type`
- `[dp]` / `[pdat]` -> `publication_date`
- `[pmid]` -> `pmid`

补充说明：

1. 默认无字段查询走 `multi_match`
2. `*` 只实现了后缀形式
3. 日期字段只支持精确值，不支持区间
4. proximity 目前只对 `"phrase"[tiab:~N]` 生效，且只查 `abstract_text`

## 8. OpenSearch 接入策略

[search/opensearch_client.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/opensearch_client.py) 负责所有 OpenSearch 访问。

当前策略：

1. API 启动时等待 OpenSearch 可用
2. 如果实际索引不存在，则创建 `articles_core_v2`（可配置）
3. 如果别名不存在，则创建 `articles_current -> articles_core_v2`
4. 导入时统一写别名，不直接写裸索引名

bulk 导入支持：

- `chunk_size`
- `max_chunk_bytes`
- `thread_count`
- 导入前临时关闭 refresh 和副本
- 导入后恢复 settings 并 refresh

当前没有实现：

- 导入任务编排层（如调度、分批计划、重试策略编排）
- 增量删除或墓碑处理
- 数据删除同步

## 9. PostgreSQL 的实际用途

代码里 PostgreSQL 只承担一件事：保存 `saved_queries`。

现状：

- 启动时建表
- 仓储层已经实现 CRUD
- 但当前 API 没有暴露相关接口

这意味着 PostgreSQL 是启动硬依赖，但还没有成为实际用户功能的一部分。

后续如果希望简化部署，有两个方向：

1. 真正把 `saved_queries` API 做完，让 PostgreSQL 的存在有业务价值
2. 如果短期不需要保存查询，移除 PostgreSQL 依赖

## 10. 配置覆盖关系

配置基础值来自 [config/app.yaml](/Users/jerry/Documents/code/medical/pmc_retrieval/config/app.yaml)。

环境变量覆盖逻辑在 [app/config.py](/Users/jerry/Documents/code/medical/pmc_retrieval/app/config.py)：

- `PG_DSN`
- `OPENSEARCH_HOSTS`
- `OS_USERNAME`
- `OS_PASSWORD`

`docker-compose.yml` 里对 `api` 还额外做了这一层覆盖：

- `OPENSEARCH_HOSTS=http://opensearch:9200`

这使得容器内 API 总是优先访问 Docker 网络内的 `opensearch` 服务，而不是 YAML 里的 `localhost`。

## 11. 已知限制和风险

### 11.1 运行层面

1. 默认是单节点 OpenSearch，不是高可用部署
2. `9200` 和 `5445` 默认暴露到宿主机
3. `healthz` 只表示进程存活，不表示索引中已有数据

### 11.2 数据与检索层面

1. `MeshExpander` 还是空实现
2. `pmid` 尚未从原始 JSON 提取
3. 日期检索只支持精确匹配
4. proximity 只落在 `abstract_text`
5. 当前没有增量同步和删除同步机制

### 11.3 工程层面

1. 查询解析测试还比较薄
2. 没有集成测试覆盖真实 OpenSearch / PostgreSQL
3. 没有导入监控、指标和重试体系

## 12. 开发时常见改动点

### 12.1 新增可检索字段

通常需要同时改：

1. [ingest/document_builder.py](/Users/jerry/Documents/code/medical/pmc_retrieval/ingest/document_builder.py)
2. [search/mapping.json](/Users/jerry/Documents/code/medical/pmc_retrieval/search/mapping.json)
3. [parser/translator.py](/Users/jerry/Documents/code/medical/pmc_retrieval/parser/translator.py)，如果该字段参与检索语法
4. 测试
5. README

### 12.2 调整查询权重

权重来源：

- YAML: `search.weights.*`
- 使用位置：[search/search_service.py](/Users/jerry/Documents/code/medical/pmc_retrieval/search/search_service.py)

如果只是调 relevance，优先动配置，不要先改 DSL 结构。

### 12.3 更换索引版本

当前已经有 `index_name` 和 `index_alias` 的区分，并在网关层支持 alias 切换；仍缺作业化迁移流程。

如果要安全升级 mapping，建议补齐：

1. 新索引建索引脚本
2. 全量重建脚本
3. 别名切换脚本
4. 回滚方案

## 13. 测试现状

当前测试位于 `tests/`，覆盖：

- 日期解析
- JSON 导入行为
- PubMed 解析器和 DSL 翻译的基础路径

缺失的关键测试：

1. `lookup_service` 查询构造
2. `search_service` 端到端响应整形
3. `saved_queries` 仓储集成测试
4. Docker/Compose 级联启动验证

## 14. 建议的下一步演进

如果要把这个服务长期跑在服务器上，优先级建议如下：

1. 给 OpenSearch 增加持久卷
2. 增加导入作业文档化或自动化
3. 补真实依赖的集成测试
4. 决定 PostgreSQL 是继续做完还是移除
5. 补索引版本迁移与别名切换工具
