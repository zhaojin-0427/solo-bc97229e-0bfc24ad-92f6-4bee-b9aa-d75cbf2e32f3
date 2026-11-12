"""SQLite 持久层：连接管理、表结构。

所有连接均使用 autocommit(isolation_level=None),写操作通过显式
BEGIN IMMEDIATE / COMMIT 控制事务边界,保证:
  - 增量确认 = increments 日志 + results 应用,同一事务提交,崩溃不丢;
  - 全量装载的分块写入与进度记录在同一事务,断点可续;
  - 基线切换(合并+替换)在单个事务内完成,读接口只见切换前或切换后。
"""
import os
import sqlite3

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    kind            TEXT NOT NULL,              -- 'full' 全量 | 'inc' 增量
    filename        TEXT NOT NULL,              -- 服务器上保存的 CSV 路径
    orig_name       TEXT,                       -- 原始文件名
    status          TEXT NOT NULL,              -- full: loading|staging_ready|done|failed|canceled
                                                -- inc : processing|done|failed
    total_bytes     INTEGER NOT NULL DEFAULT 0,
    processed_bytes INTEGER NOT NULL DEFAULT 0, -- 已处理的文件偏移(断点续传游标)
    total_rows      INTEGER NOT NULL DEFAULT 0,
    applied_rows    INTEGER NOT NULL DEFAULT 0,
    error_rows      INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    start_seq       INTEGER NOT NULL DEFAULT 0, -- 全量装载开始时的增量序号水位(当时 MAX(seq))
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 同一时刻只允许一个未完成的全量装载(数据库级强制);
-- 取消后批次转入终态 canceled,不再占用该唯一约束,可立即上传新全量。
CREATE UNIQUE INDEX IF NOT EXISTS one_active_full_load
    ON batches(kind) WHERE kind = 'full' AND status IN ('loading', 'staging_ready');

-- 正式结果表:撤回的行保留为墓碑(active=0),记录撤回修订号,
-- 使“旧修订不得恢复结果”的规则在基线切换后仍然成立。
CREATE TABLE IF NOT EXISTS results (
    report_no     TEXT NOT NULL,
    item          TEXT NOT NULL,
    result        TEXT,
    revision      INTEGER NOT NULL DEFAULT 0,
    withdrawn_rev INTEGER NOT NULL DEFAULT 0,
    active        INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (report_no, item)
);

-- 全量装载暂存区:按批次隔离(batch_id 进主键)。
-- 旧批次被取消后,其后到/迟到的写入只会命中自己的批次命名空间,
-- 永远不能写入或清空新批次的暂存区;取消时按 batch_id 清理本批残留。
CREATE TABLE IF NOT EXISTS results_staging (
    batch_id      INTEGER NOT NULL REFERENCES batches(id),
    report_no     TEXT NOT NULL,
    item          TEXT NOT NULL,
    result        TEXT,
    revision      INTEGER NOT NULL DEFAULT 0,
    withdrawn_rev INTEGER NOT NULL DEFAULT 0,
    active        INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (batch_id, report_no, item)
);

-- 已确认增量日志:seq 为全局单调序号,切换时按 seq 回放合入
CREATE TABLE IF NOT EXISTS increments (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id),
    report_no   TEXT NOT NULL,
    item        TEXT NOT NULL,
    action      TEXT NOT NULL,                  -- 'correct' | 'withdraw'
    result      TEXT,
    revision    INTEGER NOT NULL,
    effect      TEXT,                           -- applied|stale|withdrawn|recorded
    replay_required INTEGER NOT NULL DEFAULT 0, -- 1=确认期间所在全量批次未切换(取消/失败),
                                                -- 下一次成功切换必须回放,防止已确认增量丢失
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def db_path(data_dir):
    return os.path.join(data_dir, "lims.db")


def connect(data_dir):
    """新建一个连接(每线程各自使用)。autocommit + 显式事务。"""
    os.makedirs(data_dir, exist_ok=True)
    conn = sqlite3.connect(db_path(data_dir), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")      # 读写并发:装载/切换期间读接口不阻塞
    conn.execute("PRAGMA busy_timeout=30000")    # 写者排队而不是立即报锁
    conn.execute("PRAGMA synchronous=FULL")      # 已确认事务断电不丢
    return conn


def init_schema(data_dir):
    conn = connect(data_dir)
    try:
        conn.executescript(SCHEMA)
        _migrate_staging_batch_id(conn)
        _migrate_increment_replay_required(conn)
        _drop_obsolete_batch_column(conn)
    finally:
        conn.close()


def _migrate_increment_replay_required(conn):
    """旧库 increments 缺少 replay_required:补列,默认 0(无遗留待回放标记)。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(increments)")}
    if "replay_required" not in cols:
        conn.execute("ALTER TABLE increments ADD COLUMN replay_required"
                     " INTEGER NOT NULL DEFAULT 0")


def _drop_obsolete_batch_column(conn):
    """清理开发过程中引入、最终由 increments.replay_required 取代的临时列。

    SQLite 不支持 DROP COLUMN(3.35 之前)且无需保留该列数据,用整表重建;
    生产旧版本(仅有已发布的初始 schema)不存在此列,为幂等空操作。
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)")}
    if "replay_pending" not in cols:
        return
    conn.execute("ALTER TABLE batches RENAME TO batches_old")
    conn.execute(
        """
        CREATE TABLE batches (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            kind            TEXT NOT NULL,
            filename        TEXT NOT NULL,
            orig_name       TEXT,
            status          TEXT NOT NULL,
            total_bytes     INTEGER NOT NULL DEFAULT 0,
            processed_bytes INTEGER NOT NULL DEFAULT 0,
            total_rows      INTEGER NOT NULL DEFAULT 0,
            applied_rows    INTEGER NOT NULL DEFAULT 0,
            error_rows      INTEGER NOT NULL DEFAULT 0,
            error           TEXT,
            start_seq       INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)
    conn.execute(
        "INSERT INTO batches (id, kind, filename, orig_name, status, total_bytes,"
        " processed_bytes, total_rows, applied_rows, error_rows, error, start_seq,"
        " created_at, updated_at) SELECT id, kind, filename, orig_name, status,"
        " total_bytes, processed_bytes, total_rows, applied_rows, error_rows, error,"
        " start_seq, created_at, updated_at FROM batches_old")
    conn.execute("DROP TABLE batches_old")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS one_active_full_load ON batches(kind)"
        " WHERE kind = 'full' AND status IN ('loading', 'staging_ready')")


def _migrate_staging_batch_id(conn):
    """旧版本 results_staging 没有 batch_id 列:原地重建为按批次隔离的表。

    升级时若仍有装载中的暂存数据,归属到该未完成批次,保持可续装;
    无归属(理论上仅残留垃圾行)时挂到 id=0,不影响正式表与新批次。
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(results_staging)")}
    if "batch_id" in cols:
        return
    owner = conn.execute(
        "SELECT id FROM batches WHERE kind='full'"
        " AND status IN ('loading','staging_ready') ORDER BY id LIMIT 1").fetchone()
    owner_id = owner["id"] if owner else 0
    conn.execute("ALTER TABLE results_staging RENAME TO results_staging_old")
    conn.execute(
        """
        CREATE TABLE results_staging (
            batch_id      INTEGER NOT NULL REFERENCES batches(id),
            report_no     TEXT NOT NULL,
            item          TEXT NOT NULL,
            result        TEXT,
            revision      INTEGER NOT NULL DEFAULT 0,
            withdrawn_rev INTEGER NOT NULL DEFAULT 0,
            active        INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (batch_id, report_no, item)
        )
        """)
    conn.execute(
        "INSERT INTO results_staging (batch_id, report_no, item, result, revision,"
        " withdrawn_rev, active) SELECT ?, report_no, item, result, revision,"
        " withdrawn_rev, active FROM results_staging_old", (owner_id,))
    conn.execute("DROP TABLE results_staging_old")


def get_db():
    """请求作用域连接(Flask g)。"""
    if "db" not in g:
        g.db = connect(current_app.config["DATA_DIR"])
    return g.db


def close_db(_exc=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()
