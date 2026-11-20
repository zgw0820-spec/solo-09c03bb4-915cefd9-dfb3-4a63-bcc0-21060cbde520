# 计费结算两阶段提交（2PC）运维台

计费写入依赖两个 PostgreSQL 库：**扣款库（debit-db）** 与 **入账库（credit-db）**。
协调进程用两阶段提交保证两边一致，并在 `xact_log` 中留下协调日志。当协调进程在
两个预设故障点退出后，值班员在运维网页上按**结算号**查看两库残留的预备事务
（`pg_prepared_xacts`）和协调日志，选择一笔中断的结算恢复。

## 恢复决策规则

| 日志/状态证据 | 动作 |
| --- | --- |
| 任一库 `xact_log` 有 `decision_commit` | 对两库补齐 `COMMIT PREPARED`（已结束的一侧幂等跳过） |
| 任一库有 `decision_abort` | 对两库补齐 `ROLLBACK PREPARED` |
| 无任何决定，且两库都还持有预备事务 | 视为尚无决定 → 两库 `ROLLBACK PREPARED`（撤销，不扣款/入账） |
| 无任何决定，且只有一库还持有预备事务（无法判断另一侧当年是提交还是回滚） | **保留预备事务不动**，页面返回 `held` 并提示人工核实 |
| 两库均无预备事务 | 不再做任何写操作；按流水是否存在判定已完成/无此结算 |

只有两库收尾（均不再持有该预备事务）后，页面才显示「已提交 / 已撤销」。

## 全额冲正（运维页第 ④ 区）

财务发现一笔已完成的结算扣错款时，在运维页填写**原结算号**和**新冲正单号**办理全额冲正。

**受理条件**（任一不满足返回 HTTP 409）：

- 原结算号在两库都已有**已提交**的流水（普通结算），且金额一致；
  扣款库须为 `DEBIT`、入账库须为 `CREDIT`；
- 原结算号本身不能是冲正单（不允许对冲正单再冲正）；
- 同一原单**最多成功冲正一次**（数据库部分唯一索引
  `uq_reversals_committed_original` 兜底）；
- 冲正单号不能已被别的原单受理、不能是已有流水/预备事务/协调日志的普通结算号，
  也不能与原结算号相同。

**冲正结果**：两库各记一笔与原始流水**等额、方向相反**的流水，原流水保留。

| 库 | 原流水 | 冲正流水 |
| --- | --- | --- |
| 扣款库 debit | `DEBIT` | `CREDIT` |
| 入账库 credit | `CREDIT` | `DEBIT` |

**冲正单号登记表 `reversals`**（两库各一份，在任何业务事务 PREPARE 之前写入）：

- 已受理的冲正单号被登记后，普通结算**不能占用**该号
  （`POST /api/settlement/start` 与 `/api/settlement/recover` 均返回 409）；
- 同一原单已有冲正单在办理时，再次办理会被拒绝，**错误信息中直接给出该冲正单号**；
- 登记状态 `pending / committed / aborted`，终态只在两库都收尾后写入，
  真值仍以两库流水与 `xact_log` 为准（`list_reversals_view` 会重新判定）。

**中断恢复**：冲正复用同一套两阶段提交与故障点（GID 前缀 `revers_`，
流水键为冲正单号），因此协调进程在任一故障点退出后，都可用
`POST /api/reversal/recover`（**只需冲正单号**，原单号从登记表回查）从原入口恢复：

| 冲正证据 | 恢复动作 |
| --- | --- |
| 两库都已有冲正流水 | `committed`（补齐登记） |
| 任一库有 `decision_commit` | 两库补齐 `COMMIT PREPARED` |
| 任一库有 `decision_abort` / 两库均确认 `aborted` | 两库补齐 `ROLLBACK PREPARED` |
| 无决定且两库都仍预备 | `ROLLBACK PREPARED`（撤销） |
| 无决定且仅一库仍预备 | **保留**，返回 `held` |
| 无决定、无流水、仅登记了冲正号 | 两库写入撤销确认，返回 `aborted`，允许**换号重试** |
| 无决定、仅一库仍预备、另一库无任何证据（两库 PREPARE 之间崩溃） | 回滚该预备事务，两库写入撤销确认，返回 `aborted`，允许换号重试 |
| 有提交决定但缺冲正流水 | 返回 `held`，需人工核实 |

**换号重试**：只有两库都确认撤销且都没有冲正流水（`aborted`），才允许为原单
用**新的**冲正单号重试；已撤销的冲正单号本身不能复用。

**页面查看**：第 ④ 区「查看关联冲正」按原结算号列出该原单的所有冲正单号、
登记状态、处理结果（成功/撤销/办理中/保留）以及两库流水和协调日志。

## 防重复扣款、入账与并发保护

1. `ledger_entries(settlement_no)` 上有**唯一索引**，同号重复写入会被数据库直接拒绝。
2. 恢复入口先在扣款库取**咨询锁**（`pg_try_advisory_lock`，键含结算号）：同号并发/重复
   恢复只有一个请求能拿到锁，其余立即返回 `duplicate`，不做任何写操作。
3. 冲正按**原结算号**取咨询锁，同一原单的并发冲正只有一个能进入；冲正单号另取
   一把 `settle_number` 锁，与普通结算互斥（谁先拿到锁，谁就能看到对方的登记/流水）。
4. `reversals(original_no) WHERE status='committed'` 是部分唯一索引，
   数据库层面保证同一原单最多一笔成功冲正。
5. `COMMIT/ROLLBACK PREPARED` 对"事务已不存在"做幂等处理（`already_gone`），崩溃后重放安全。
6. 发起结算前检查两库是否已存在同号预备事务、流水，或被受理为冲正单号，
   存在则拒绝（HTTP 409），要求走恢复。

## 两个可注入的中断点

由环境变量 `FAILPOINT` 控制（默认 `none`）：

- `after_both_prepared`：两库都已 `PREPARE TRANSACTION`、协调日志只有 `prepared`、
  **尚未写入任何决定**时，协调进程 `os._exit(99)` 硬退出。重启后恢复 → 撤销。
- `after_one_commit`：扣款库已 `COMMIT PREPARED`、两库已写 `decision_commit`、
  入账库仍持有预备事务时硬退出。重启后恢复 → 补齐入账库提交。

## 方式一：Docker Compose 演练（推荐交付路径）

```bash
# 演练中断点一（两库已预备、尚未决定）
FAILPOINT=after_both_prepared docker compose up --build
# 浏览器打开 http://localhost:8080
#   1) 在②区填结算号（如 S1001）和金额，点“发起两阶段结算”，ops-web 进程会退出
#   2) docker compose up -d 重新拉起（FAILPOINT 可改回 none）
#   3) 在③区输入同一结算号，先“查看日志”再“恢复此结算” → 状态为 aborted
#
# 中断点二把 FAILPOINT 换成 after_one_commit，恢复后状态为 committed
FAILPOINT=none docker compose up -d

# 冲正演练（同一套故障点也作用于冲正）：
#   1) 先正常发起并完成一笔普通结算（如 S1001 / 100.00）
#   2) 在④区填原结算号 S1001、冲正单号 R1001，点“办理全额冲正”
#   3) 重启后（FAILPOINT=none），在④区只填冲正单号 R1001，点“按冲正单号恢复”
#      → after_both_prepared 恢复为 aborted（可换号重试）
#      → after_one_commit    恢复为 committed（补齐另一库）
#   4) 点“查看关联冲正”可按原结算号看到关联冲正单号与处理结果
```

- 扣款库映射宿主端口 55432，入账库 55433，网页 8080。
- 两个库都显式设置 `max_prepared_transactions=200`（PostgreSQL 默认关闭预备事务）。
- 数据存放在命名卷 `debit-data` / `credit-data`，容器重启后预备事务与日志都保留。
- 清理：`docker compose down -v`。

## 方式二：无 Docker 的本地等价演练

本仓库还提供基于 `pgserver` wheel（内含完整 PostgreSQL 16 二进制）的双实例脚本，
用于没有容器运行时的环境，行为与 Compose 双库等价。

```bash
python -m venv --without-pip .venv && .venv/bin/python .local-setup/get-pip.py   # 仅首次引导
.venv/bin/python -m pip install -r requirements.txt pgserver httpx
# 启动两个本地 PG 实例（.local-pg/，端口 55432/55433）
.venv/bin/python -I scripts/local_pg.py start
.venv/bin/python -I scripts/local_pg.py status

# 带故障点启动网页
set -a && . .local-pg/state.env && set +a
FAILPOINT=after_both_prepared DSN_DEBIT="$DSN_DEBIT" DSN_CREDIT="$DSN_CREDIT" \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8080
# 进程退出后用 FAILPOINT=none 重新执行同一条 uvicorn 命令即可“重启继续”

# 一键自测（含两处中断的崩溃→重启→恢复、并发、保留、正常路径）
.venv/bin/python -I scripts/self_test.py
```

## 目录结构

```
app/
  config.py       环境配置、结算号/GID 校验
  db.py           双库同构 schema、预备事务与协调日志操作、咨询锁
  coordinator.py  2PC 发起、故障注入、按日志恢复（提交/撤销/保留）
  main.py         FastAPI 路由与运维网页
  templates/index.html
scripts/
  local_pg.py     无 Docker 时的双 PG 实例管理
  self_test.py    端到端自测（98 项断言，含冲正并发/中断恢复）
docker-compose.yml / Dockerfile / requirements.txt
```

## API 摘要

- `GET  /` 运维网页
- `GET  /api/state` 两库残留预备事务
- `GET  /api/settlement/{no}` 该结算号在两库的协调日志与流水情况
- `POST /api/settlement/start`（表单 `settlement_no`, `amount`）发起两阶段结算
- `POST /api/settlement/recover`（表单 `settlement_no`）恢复
- `GET  /api/reversal/{original_no}` 该原结算关联的所有冲正单及处理结果
- `POST /api/reversal/start`（表单 `original_no`, `reversal_no`）办理全额冲正
- `POST /api/reversal/recover`（表单 `reversal_no`，可选 `original_no`）按冲正单号恢复
- `GET  /healthz`
