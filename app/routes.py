"""HTTP 接口:上传、进度、取数、页面。"""
import os
import sqlite3
import uuid

from flask import (Blueprint, current_app, jsonify, render_template, request)

from . import db, loader, logic

bp = Blueprint("main", __name__)

SAMPLE_FILES = {
    "full_baseline": "full_baseline.csv",
    "full_baseline_v2": "full_baseline_v2.csv",
    "full_large": "full_baseline_large.csv",
    "inc_corrections": "inc_corrections.csv",
    "inc_withdrawals": "inc_withdrawals.csv",
    "inc_after_withdraw": "inc_after_withdraw.csv",
    "inc_during_load": "inc_during_load.csv",
}


def _err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def _save_upload(file_storage):
    uploads = os.path.join(current_app.config["DATA_DIR"], "uploads")
    os.makedirs(uploads, exist_ok=True)
    orig = file_storage.filename or "upload.csv"
    path = os.path.join(uploads, f"{uuid.uuid4().hex}.csv")
    file_storage.save(path)
    return path, orig


def _create_full_batch(path, orig):
    """登记全量批次;数据库部分唯一索引保证同一时刻仅一个装载。

    暂存区按 batch_id 隔离,无需清空:取消/失败旧批的残留只属于旧批,
    新批有独立命名空间,旧执行的迟到写入不可能污染本批暂存区。
    start_seq 记录装载开始时的增量水位,用于界定“装载期间增量”。
    """
    conn = db.get_db()
    size = os.path.getsize(path)
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes, start_seq)"
            " VALUES ('full', ?, ?, 'loading', ?, ?)",
            (path, orig, size, logic.max_increment_seq(conn)))
        conn.execute("COMMIT")
        return cur.lastrowid
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK")
        raise
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _create_inc_batch(path, orig):
    conn = db.get_db()
    size = os.path.getsize(path)
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes)"
            " VALUES ('inc', ?, ?, 'processing', ?)", (path, orig, size))
        conn.execute("COMMIT")
        return cur.lastrowid
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _batch_json(row):
    total = row["total_bytes"] or 0
    done = row["processed_bytes"] or 0
    return {
        "id": row["id"], "kind": row["kind"], "status": row["status"],
        "orig_name": row["orig_name"], "error": row["error"],
        "total_rows": row["total_rows"], "applied_rows": row["applied_rows"],
        "error_rows": row["error_rows"],
        "progress": round(100.0 * done / total, 1) if total else 0.0,
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }


# ------------------------------------------------------------------ 页面

def _poke(app):
    """唤醒后台装载线程;测试模式下由测试用例手动驱动装载。"""
    if not app.config.get("TESTING"):
        loader.poke_background(app)


@bp.get("/")
def index():
    return render_template("index.html", samples=sorted(SAMPLE_FILES.keys()))


# ------------------------------------------------------------------ 上传

@bp.post("/upload/full")
def upload_full():
    f = request.files.get("file")
    if f is None or not f.filename:
        return _err("请选择要上传的全量 CSV 文件")
    path, orig = _save_upload(f)
    try:
        batch_id = _create_full_batch(path, orig)
    except sqlite3.IntegrityError:
        os.unlink(path)
        return _err("已有一个全量装载正在进行,请等待其完成后再上传", 409)
    _poke(current_app._get_current_object())
    return jsonify({"ok": True, "batch_id": batch_id,
                    "message": f"全量批次 #{batch_id} 已开始后台装载"})


@bp.post("/upload/inc")
def upload_inc():
    f = request.files.get("file")
    if f is None or not f.filename:
        return _err("请选择要上传的增量 CSV 文件")
    path, orig = _save_upload(f)
    batch_id = _create_inc_batch(path, orig)
    try:
        # 增量在请求内同步确认:响应返回时已立即更新正式表
        loader.process_inc_batch(db.get_db(), batch_id, current_app.config["CHUNK_SIZE"])
    except logic.CsvFormatError as exc:
        loader.mark_failed(current_app.config["DATA_DIR"], batch_id, exc)
        return _err(f"增量文件格式错误: {exc}")
    batch = db.get_db().execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    return jsonify({"ok": True, "batch": _batch_json(batch),
                    "message": f"增量批次 #{batch_id} 已确认并应用到正式结果表"})


@bp.post("/load_sample/<name>")
def load_sample(name):
    """一键载入随附示例文件(演示装载中更正/撤回)。"""
    if name not in SAMPLE_FILES:
        return _err("未知示例", 404)
    path = os.path.join(current_app.config["SAMPLES_DIR"], SAMPLE_FILES[name])
    if not os.path.exists(path):
        return _err(f"示例文件不存在: {SAMPLE_FILES[name]}", 404)
    orig = f"sample:{SAMPLE_FILES[name]}"
    if name.startswith("full"):
        try:
            batch_id = _create_full_batch(path, orig)
        except sqlite3.IntegrityError:
            return _err("已有一个全量装载正在进行,请等待其完成", 409)
        _poke(current_app._get_current_object())
        return jsonify({"ok": True, "batch_id": batch_id,
                        "message": f"示例全量批次 #{batch_id} 已开始后台装载"})
    batch_id = _create_inc_batch(path, orig)
    try:
        loader.process_inc_batch(db.get_db(), batch_id, current_app.config["CHUNK_SIZE"])
    except logic.CsvFormatError as exc:
        loader.mark_failed(current_app.config["DATA_DIR"], batch_id, exc)
        return _err(f"增量文件格式错误: {exc}")
    batch = db.get_db().execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    return jsonify({"ok": True, "batch": _batch_json(batch),
                    "message": f"示例增量批次 #{batch_id} 已确认并应用"})


# ------------------------------------------------------------------ 取消

@bp.post("/api/batches/<int:batch_id>/cancel")
def cancel_batch(batch_id):
    """取消全量批次:停止后续装载、增量回放与切换,保留现有正式结果。

    - 仅适用于装载中(loading)、待切换(staging_ready)的全量批次;
    - 已切换(done)拒绝取消——取消与切换至多一个生效;
    - 重复取消幂等:返回同一 canceled 终态;
    - 取消后立即释放单装载约束,可马上上传新全量;旧批迟到处理只写本批
      命名空间(已清理),不会写入或清空新批暂存区。
    """
    conn = db.get_db()
    batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    if batch is None:
        return _err(f"批次 #{batch_id} 不存在", 404)
    if batch["kind"] != "full":
        return _err(f"批次 #{batch_id} 是增量批次,取消仅适用于全量批次", 400)
    if batch["status"] == "canceled":
        return jsonify({"ok": True, "already": True,
                        "batch": _batch_json(batch),
                        "message": f"全量批次 #{batch_id} 已处于取消终态"})
    if batch["status"] == "done":
        return _err(f"批次 #{batch_id} 已完成基线切换,不能取消(切换与取消只能有一个生效)", 409)
    if batch["status"] == "failed":
        return _err(f"批次 #{batch_id} 已失败,无需取消", 409)

    conn.execute("BEGIN IMMEDIATE")
    try:
        moved = logic.cancel_full_batch(conn, batch_id)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    if not moved:
        # 并发竞争:事务内状态已被切换事务改变
        fresh = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        return _err(f"批次 #{batch_id} 状态已变为 {fresh['status']},取消未生效", 409)
    fresh = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    return jsonify({"ok": True, "already": False, "batch": _batch_json(fresh),
                    "message": f"全量批次 #{batch_id} 已取消:停止装载与切换,"
                               f"正式结果及已确认增量保持不变,可立即换传新全量"})


# ------------------------------------------------------------------ 取数与进度

@bp.get("/api/results")
def api_results():
    """报告系统取数接口:单条 SELECT,保证每次读到切换前或后的完整有效结果。"""
    conn = db.get_db()
    report_no = request.args.get("report_no") or None
    include_withdrawn = request.args.get("all") == "1"
    rows = logic.current_results(conn, report_no=report_no,
                                 include_withdrawn=include_withdrawn)
    results = [{
        "report_no": r["report_no"], "item": r["item"], "result": r["result"],
        "revision": r["revision"],
        **({"active": bool(r["active"]), "withdrawn_rev": r["withdrawn_rev"]}
           if include_withdrawn else {}),
    } for r in rows]
    return jsonify({"count": len(results), "results": results})


@bp.get("/api/batches")
def api_batches():
    rows = db.get_db().execute(
        "SELECT * FROM batches ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify({"batches": [_batch_json(r) for r in rows]})


@bp.get("/api/increments")
def api_increments():
    limit = min(int(request.args.get("limit", 50)), 500)
    rows = db.get_db().execute(
        "SELECT * FROM increments ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
    return jsonify({"increments": [dict(r) for r in rows]})


@bp.get("/api/status")
def api_status():
    conn = db.get_db()
    active = conn.execute(
        "SELECT COUNT(*) AS c FROM results WHERE active=1").fetchone()["c"]
    withdrawn = conn.execute(
        "SELECT COUNT(*) AS c FROM results WHERE active=0").fetchone()["c"]
    inc = conn.execute("SELECT COUNT(*) AS c FROM increments").fetchone()["c"]
    loading = conn.execute(
        "SELECT id, status FROM batches WHERE kind='full'"
        " AND status IN ('loading','staging_ready')").fetchone()
    canceled = conn.execute(
        "SELECT COUNT(*) AS c FROM batches WHERE kind='full' AND status='canceled'"
    ).fetchone()["c"]
    return jsonify({
        "active_results": active, "withdrawn_records": withdrawn,
        "confirmed_increments": inc,
        "full_load_in_progress": dict(loading) if loading else None,
        "canceled_full_batches": canceled,
    })


@bp.get("/api/health")
def api_health():
    return jsonify({"ok": True})
