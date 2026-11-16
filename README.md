# mtloader — 多目标装载算子

清洗程序一次产出一张逻辑明细流，本算子按路由配置把它装载到多个按地区划分的
目标表，并维护一张独立的隔离（无效数据）表。某行改了地区，会从旧表消失再进
新表；某行改成无效，不会仍留在原表。

算子**不负责**源端快照、日志采集或 CDC 协议解析；它接收调用方递交的、有顺序
的逻辑变更批次，把目标关系之间的迁移语义、批次位置和路由版本在**同一个
PostgreSQL 事务**里落定。数据库是唯一的提交真相，没有本地进度文件。

## 逻辑模型

* 每个逻辑行有稳定 `row_id`、完整业务字段和当前分区。
* 变更：`Insert(row_id, fields)` / `Update(row_id, fields)`（完整后像，可更换
  业务唯一键与路由字段）/ `Delete(row_id)`。同一批次可多次修改同一 `row_id`，
  按批内顺序生效。
* 批次 `Batch(partition, position, expected_previous, changes)`：
  * `position` 在分区内严格递增（允许跳号），**首次批次必须恰好落在声明起点**；
  * `expected_previous` 是调用方认为已应用的上一位置（首批为 `None`），落库时
    以条件 UPDATE 做**原子比较**（`SET last_position ... WHERE last_position
    IS NOT DISTINCT FROM expected_previous` 并校验受影响行数），两个写者不会
    互相覆盖进度。

## 路由配置（纯数据，无任意 SQL/Python）

```python
RouteConfig(
    field_types={"bk": "text", "region": "text", "amount": "int"},
    key_fields=("bk",),                      # 客户声明的业务唯一键
    targets=(
        TargetDef("east", "t_east", {"bk": "bk_e", "region": "region", "amount": "amt"}),
        TargetDef("west", "t_west", {"bk": "bk_w", "amount": "amount"}),  # 列名/列集合可不同
    ),
    quarantine=QuarantineDef("quarantine", "t_quarantine"),
    rules=(Rule(Cmp("region", "eq", "east"), "east"),
           Rule(Cmp("region", "eq", "west"), "west")),
    default="quarantine",
)
```

* 谓词是数据 AST：`Cmp(field, op, value)`（`eq/ne/lt/le/gt/ge`，值类型必须与
  声明字段类型一致）+ `And/Or`。配置校验拒绝未知字段、未知算子、类型不符的
  字面量、非法标识符；标识符（`^[A-Za-z_][A-Za-z0-9_]{0,62}$`）再经
  `psycopg2.sql.Identifier` 安全引用，值全部走参数绑定。
* 一行**命中多支**即配置冲突：该行进隔离表，原因 `route_multi_match`——
  系统检测并隔离，不用规则顺序掩盖冲突。其它非法行：`no_matching_rule`
  （无命中且默认是隔离）、`missing_key`（业务键缺失）、
  `field_type_mismatch:<field>`。
* 合法行恰好属于一个业务目标；不合法行只属于隔离目标，隔离表保存完整后像
  （jsonb）与原因。

## 数据库布局（同一事务内落定）

| 表 | 作用 |
|---|---|
| `mt_ownership` | 每 `row_id` 当前归属目标 + 完整规范行（jsonb）+ 业务键（隔离行为 NULL） |
| `mt_partition_state` | 每分区声明起点、最后已应用位置与摘要 |
| `mt_applied_batches` | 每分区每位置的完整内容摘要、commit_seq、路由版本 |
| `mt_meta` | 路由版本、全局 commit_seq、当前路由配置、schema 指纹 |
| `mt_route_history` | 每次 reroute 的版本与配置 |
| 目标表 / 隔离表 | 由配置生成；只含 `row_id` 主键，**无**局部业务键 UNIQUE |

业务唯一键的唯一性由 `mt_ownership` 上的
`UNIQUE (biz_key) DEFERRABLE INITIALLY DEFERRED` 保证：**整批最终状态**判定，
中间操作不制造假冲突——因此一批里两个 `row_id` 可以交换唯一键，也可以先删旧
行再插另一行沿用该键。隔离行 `biz_key` 为 NULL，不占此键。提交前还有一次
事务内预检，产出带 `row_id` 与键摘要（不含原始业务值）的清晰错误；延迟约束
是兜底。物理目标表刻意不放局部 UNIQUE，否则批内换键会被中间状态误伤。

## 一致性边界与并发约定

* 所有写操作（`apply` / `reroute` / `declare_partition`）先取
  `pg_advisory_xact_lock`（固定 key），**整次操作串行化**；随后在同一事务里
  完成全部目标表、归属、位置、摘要、路由版本的写入并提交。
* 读者不加锁：MVCC 快照保证任何连接只看到完整批次（`snapshot()` 用
  REPEATABLE READ 一次性读出逻辑表 + 各目标 + 位置 + 路由版本）。
* `reroute` 与正常批次共享同一把锁与同一事务纪律：不存在半库旧规则半库新
  规则的窗口。`apply` 每次从 `mt_meta` 读取当前已提交的路由配置，多个算子
  实例不会各执旧版。
* 每次成功写操作递增 `commit_seq` 并随结果返回；它给所有已提交写操作一个
  全序，测试据此把并发结果重放进独立内存解释器校验。

### 重试约定

* 可重试：`RetryableError`（序列化失败/死锁）、连接级错误
  （`psycopg2.OperationalError/InterfaceError`）。**必须用同一个批次对象**
  重试——若上一次实际已提交而响应丢失，摘要幂等会让重放返回
  `already_applied=True` 且零副作用。`apply_with_retry` 实现了该约定。
* 不可重试（确定性拒绝，原样重试必然再失败）：`KeyConflict`（最终状态撞
  键）、`PositionConflict`（同位置异摘要/位置已埋入历史/首批未落在起点）、
  `StaleExpectedPrevious`（需重新同步进度后构造**新**批次）、`BatchError`
  （批次本身非法）、`BatchFailed`（批中 SQL 错误，已完整回滚）。
* 死锁、序列化失败、约束异常一律不对外报告成功；失败完整回滚，不改变任何
  当前归属与位置。

## API 一览

```python
loader = MultiTargetLoader(conn_factory)          # conn_factory: () -> psycopg2 connection
loader.init_schema(config, {"P": 100})            # 建表 + 声明路由配置与各分区起点（幂等）
loader.declare_partition("Q", 1)                  # 事后增补分区
res  = loader.apply(batch)                        # ApplyResult(commit_seq, route_version, already_applied, ...)
res  = loader.apply_with_retry(batch)             # 上述重试约定
r    = loader.reroute(new_config)                 # 换版重算并迁移；保留 row_id，不改业务字段
rows = loader.read_logical()                      # 逻辑当前表（row_id/归属/键/完整字段）
rows = loader.read_target("east")                 # 某目标物理行（含隔离表：row_id/原因/后像）
st   = loader.partition_status()                  # 各分区起点/最后位置/摘要
ver  = loader.route_version()
snap = loader.snapshot()                          # 一致快照读取全部
cfg  = loader.current_config()                    # 当前已提交的路由配置
```

`reroute` 只允许更换 `rules`/`default`（目标、键字段、字段类型构成 schema
指纹，变更须重建）。迁移按新规则重算 `mt_ownership` 里的完整规范行并物理搬
迁；若换版后某隔离行变合法而其键与他行冲突，整个 reroute 被拒绝并回滚。

## 最小数据库权限

运行账号只需要 DML，不需要 DDL 与任何特殊权限：

```sql
CREATE ROLE mtloader LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE appdb TO mtloader;
GRANT USAGE ON SCHEMA public TO mtloader;
-- 由初始化账号（或迁移工具）先执行 init_schema 后：
GRANT SELECT, INSERT, UPDATE, DELETE ON
    mt_meta, mt_partition_state, mt_applied_batches, mt_ownership, mt_route_history,
    t_east, t_west, t_quarantine            -- 配置声明的全部目标表与隔离表
TO mtloader;
```

说明：算子不用序列/触发器/临时表，`pg_advisory_xact_lock` 无需额外授权；
`init_schema`/`drop_schema` 需要 CREATE/DROP 权限，建议交给迁移账号执行。
日志只记录分区、位置、行数、`row_id` 与键摘要，不输出完整业务行。

## 故障点（仅测试用）

`MultiTargetLoader(..., fault_hook=fn)` 在写事务内的 `after_lock` 与
`before_commit` 两个点回调，用于**观察事务边界**（阻塞以验证读者所见、或抛
错以验证完整回滚）。没有也不需任何额外恢复账本：失败的操作从未提交。

## 演练（本机临时 PostgreSQL 实例）

```bash
scripts/pg_start.sh        # 初始化并启动临时实例（unix socket，端口 55432）
venv/bin/python -m pytest  # 全部 16 个测试在真实 PostgreSQL 上运行（非 SQLite 模拟）
scripts/pg_stop.sh
```

测试覆盖：A/B/C 三行初始落表 → 一批内交换 A/B 业务键 + A 迁西区 + C 变合法
→ B 改非法 + 删 A 并复用其键；跨目标最终撞键；同批多次改同 row_id；重复同
摘要/异摘要；过期 expected_previous；路由多命中；新版 reroute（含撞键拒绝）；
途中 SQL 错误回滚；提交边界处读者一致性；两连接争同键；reroute 与 apply 两
种确定顺序 + 屏障竞速；重试包装。每个检查点把**真实查询结果**与独立内存解
释器的预期比对，并保存到 `artifacts/*.json`。
