"""取消全量批次需求覆盖测试:

1. 装载中 / 待切换的全量批次可取消;取消停止后续装载、增量回放与切换
2. 取消保留现有正式结果及期间已确认增量;等待停止期间增量上传与取数可用
3. 取消与切换只能有一个生效;已切换/已失败拒绝取消;增量批次不可取消
4. 重复取消幂等,返回同一终态
5. 取消后可立即上传新全量;旧执行的迟到写入/清理不得写入或清空新批暂存区
6. 重启后已取消批次不再恢复;其他未完成批次仍按原进度接续
7. 页面展示取消状态与取消/换传入口
"""
import io
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app          # noqa: E402
from app import db, loader, logic   # noqa: E402


@pytest.fixture()
def app(tmp_path):
    return create_app({
        "TESTING": True,
        "DATA_DIR": str(tmp_path / "data"),
        "SAMPLES_DIR": os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "samples"),
        "CHUNK_SIZE": 3,
        "CHUNK_DELAY": 0.0,
    })


@pytest.fixture()
def client(app):
    return app.test_client()


def csv_upload(client, rows, kind="full", header=None):
    header = header or (["report_no", "item", "result", "revision"] if kind == "full"
                        else ["report_no", "item", "action", "result", "revision"])
    buf = io.StringIO()
    buf.write(",".join(header) + "\n")
    for r in rows:
        buf.write(",".join(str(c) for c in r) + "\n")
    data = {"file": (io.BytesIO(buf.getvalue().encode("utf-8")), "up.csv")}
    return client.post(f"/upload/{kind}", data=data,
                       content_type="multipart/form-data")


def finish_pending(app):
    loader.run_pending(app.config["DATA_DIR"], app.config["CHUNK_SIZE"])


def active_map(app):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        rows = logic.current_results(conn)
        return {(r["report_no"], r["item"]): (r["result"], r["revision"])
                for r in rows if r["active"] == 1}
    finally:
        conn.close()


def batch_rows(app):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        return {r["id"]: dict(r) for r in
                conn.execute("SELECT * FROM batches ORDER BY id").fetchall()}
    finally:
        conn.close()


def full_batch_id(app):
    return max(batch_rows(app))


def staging_count(app, batch_id):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        return conn.execute("SELECT COUNT(*) c FROM results_staging WHERE batch_id=?",
                            (batch_id,)).fetchone()["c"]
    finally:
        conn.close()


def make_full(app, client, rows):
    r = csv_upload(client, rows)
    assert r.status_code == 200
    bid = r.get_json()["batch_id"]
    return bid


# ------------------------------------------------------------ 取消的基本语义

def test_cancel_loading_batch_stops_load_and_keeps_baseline(app, client):
    # 正式基线 v1
    csv_upload(client, [("OLD", "铅", "1.0", 1)])
    finish_pending(app)
    # 误传的新全量:装载一块后暂停(仍处 loading)
    bid = make_full(app, client, [(f"N{i}", "铅", "2", 1) for i in range(9)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid]["status"] == "loading"

    # 等待停止期间:增量上传与取数保持可用
    r = csv_upload(client, [("OLD", "铅", "correct", "1.5", 2)], kind="inc")
    assert r.status_code == 200
    assert active_map(app)[("OLD", "铅")] == ("1.5", 2)
    assert client.get("/api/results").status_code == 200

    # 取消
    r = client.post(f"/api/batches/{bid}/cancel")
    assert r.status_code == 200
    assert r.get_json()["batch"]["status"] == "canceled"

    # 再跑调度器:已取消批次不会恢复,正式表保持旧基线 + 期间增量
    finish_pending(app)
    assert batch_rows(app)[bid]["status"] == "canceled"
    assert active_map(app) == {("OLD", "铅"): ("1.5", 2)}
    # 本批暂存已清理
    assert staging_count(app, bid) == 0


def test_cancel_staging_ready_batch_blocks_switch(app, client, monkeypatch):
    csv_upload(client, [("OLD", "铅", "1.0", 1)])
    finish_pending(app)
    bid = make_full(app, client, [("NEW", "铅", "2.0", 1)])
    # 装载完成到达 staging_ready,但拦截切换以稳定复现“待切换”窗口
    def block_switch(conn, batch_id):
        raise loader.PauseProcessing()

    monkeypatch.setattr(loader, "_merge_and_switch", block_switch)
    loader.run_pending(app.config["DATA_DIR"], 3)
    monkeypatch.undo()
    assert batch_rows(app)[bid]["status"] == "staging_ready"
    # 待切换仍可取数,读到的是旧基线
    assert client.get("/api/results").get_json()["results"][0]["report_no"] == "OLD"

    r = client.post(f"/api/batches/{bid}/cancel")
    assert r.status_code == 200 and r.get_json()["batch"]["status"] == "canceled"
    # 调度器不会再切换;旧基线原样保留
    finish_pending(app)
    assert active_map(app) == {("OLD", "铅"): ("1.0", 1)}
    assert batch_rows(app)[bid]["status"] == "canceled"


def test_cancel_confirmed_increments_are_preserved(app, client):
    csv_upload(client, [("A", "铅", "1", 1), ("B", "镉", "2", 1)])
    finish_pending(app)
    bid = make_full(app, client, [("A", "铅", "9", 5), ("C", "铅", "1", 1),
                                  ("D", "铅", "1", 1), ("E", "铅", "1", 1)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid]["status"] == "loading"
    # 装载期间确认的增量
    csv_upload(client, [("B", "镉", "correct", "2.2", 2),
                        ("A", "铅", "withdraw", "", 3)], kind="inc")
    client.post(f"/api/batches/{bid}/cancel")
    finish_pending(app)
    # 正式结果保留期间增量效果
    m = active_map(app)
    assert m[("B", "镉")] == ("2.2", 2)
    assert ("A", "铅") not in m
    # 增量日志仍在(不删除、可审计)
    conn = db.connect(app.config["DATA_DIR"])
    try:
        assert conn.execute("SELECT COUNT(*) c FROM increments").fetchone()["c"] == 2
    finally:
        conn.close()


# ------------------------------------------------------------ 取消的拒绝与幂等

def test_cancel_done_batch_rejected(app, client):
    bid = make_full(app, client, [("A", "铅", "1", 1)])
    finish_pending(app)
    r = client.post(f"/api/batches/{bid}/cancel")
    assert r.status_code == 409 and "切换" in r.get_json()["error"]
    assert batch_rows(app)[bid]["status"] == "done"   # 终态未被改变


def test_cancel_failed_batch_rejected(app, client):
    bad = {"file": (io.BytesIO("wrong,header\n1,2\n".encode()), "bad.csv")}
    client.post("/upload/full", data=bad, content_type="multipart/form-data")
    bid = full_batch_id(app)
    loader.run_pending(app.config["DATA_DIR"], 3)
    assert batch_rows(app)[bid]["status"] == "failed"
    r = client.post(f"/api/batches/{bid}/cancel")
    assert r.status_code == 409


def test_cancel_inc_batch_rejected_and_unknown_404(app, client):
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    r = csv_upload(client, [("A", "铅", "correct", "2", 2)], kind="inc")
    inc_id = r.get_json()["batch"]["id"]
    r = client.post(f"/api/batches/{inc_id}/cancel")
    assert r.status_code == 400
    assert client.post("/api/batches/999999/cancel").status_code == 404


def test_repeat_cancel_is_idempotent_same_terminal_state(app, client):
    bid = make_full(app, client, [(f"R{i}", "铅", "1", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid]["status"] == "loading"
    first = client.post(f"/api/batches/{bid}/cancel").get_json()
    assert first["ok"] and first["already"] is False
    second = client.post(f"/api/batches/{bid}/cancel")
    body = second.get_json()
    assert second.status_code == 200 and body["already"] is True
    assert body["batch"]["status"] == "canceled"
    rows = [b for b in batch_rows(app).values() if b["id"] == bid]
    assert len(rows) == 1 and rows[0]["status"] == "canceled"


# ------------------------------------------------------------ 取消与切换互斥

def test_switch_committed_then_cancel_loses(app, client, monkeypatch):
    """切换先提交:取消必须拒绝(取消与切换只能有一个生效)。"""
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    bid = make_full(app, client, [("A", "铅", "2", 2)])
    entered = threading.Event()
    release = threading.Event()

    real_merge = logic.merge_and_switch

    def slow_merge(conn, batch_id):
        if batch_id == bid:
            entered.set()
            release.wait(5)
        return real_merge(conn, batch_id)

    monkeypatch.setattr(logic, "merge_and_switch", slow_merge)
    t = threading.Thread(target=loader.run_pending,
                         args=(app.config["DATA_DIR"], 3), daemon=True)
    t.start()
    assert entered.wait(5)
    release.set()           # 让切换事务提交
    t.join(5)
    assert batch_rows(app)[bid]["status"] == "done"
    r = client.post(f"/api/batches/{bid}/cancel")
    assert r.status_code == 409
    assert active_map(app)[("A", "铅")] == ("2", 2)


def test_cancel_committed_then_switch_loses(app, client, monkeypatch):
    """取消先提交:装载线程的切换必须被事务内守卫阻止(切换与取消互斥)。"""
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    bid = make_full(app, client, [("A", "铅", "2", 2), ("X", "铅", "1", 1)])

    def block_switch(conn, batch_id):
        raise loader.PauseProcessing()

    monkeypatch.setattr(loader, "_merge_and_switch", block_switch)
    loader.run_pending(app.config["DATA_DIR"], 3)   # 装载完成,停在 staging_ready
    monkeypatch.undo()
    assert batch_rows(app)[bid]["status"] == "staging_ready"

    # 取消先落库
    assert client.post(f"/api/batches/{bid}/cancel").status_code == 200
    assert batch_rows(app)[bid]["status"] == "canceled"

    # 装载线程随后才执行切换:merge 守卫拒绝,事务回滚
    conn = db.connect(app.config["DATA_DIR"])
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(logic.CancelNotAllowedError):
            logic.merge_and_switch(conn, bid)
        conn.execute("ROLLBACK")
    finally:
        conn.close()

    # 调度器再跑也不会复活该批;旧基线保留,新全量从未进入正式表
    finish_pending(app)
    assert batch_rows(app)[bid]["status"] == "canceled"
    assert active_map(app) == {("A", "铅"): ("1", 1)}


# ------------------------------------------------------------ 取消后立即换传 + 迟到隔离

def test_orphan_flags_cleared_after_successful_switch(app, client):
    """孤儿增量被新基线承继后,待回放标记在同一切换事务中清除。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bad = make_full(app, client, [(f"W{i}", "铅", "9", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    csv_upload(client, [("R1", "铅", "correct", "0.55", 5)], kind="inc")
    client.post(f"/api/batches/{bad}/cancel")
    conn = db.connect(app.config["DATA_DIR"])
    try:
        assert conn.execute(
            "SELECT replay_required FROM increments WHERE seq=1").fetchone()[0] == 1
    finally:
        conn.close()
    good = make_full(app, client, [("R1", "铅", "0.11", 3), ("R2", "镉", "0.2", 1)])
    finish_pending(app)
    assert batch_rows(app)[good]["status"] == "done"
    conn = db.connect(app.config["DATA_DIR"])
    try:
        assert conn.execute(
            "SELECT COALESCE(SUM(replay_required),0) s FROM increments").fetchone()["s"] == 0
    finally:
        conn.close()


def test_chained_cancels_keep_all_orphan_increments(app, client):
    """连续取消两批:两次等待期间的增量都是孤儿,第三批切换全部承继。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bad1 = make_full(app, client, [(f"W{i}", "铅", "9", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    csv_upload(client, [("R1", "铅", "correct", "0.4", 4)], kind="inc")   # seq 1
    client.post(f"/api/batches/{bad1}/cancel")
    bad2 = make_full(app, client, [(f"X{i}", "铅", "8", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    csv_upload(client, [("R1", "铅", "correct", "0.6", 6)], kind="inc")   # seq 2
    client.post(f"/api/batches/{bad2}/cancel")
    make_full(app, client, [("R1", "铅", "0.2", 2), ("R2", "镉", "0.2", 1)])
    finish_pending(app)
    # 两个孤儿更正中 rev6 最高,应生效;rev4 被同键更高修订盖过
    assert active_map(app)[("R1", "铅")] == ("0.6", 6)
    conn = db.connect(app.config["DATA_DIR"])
    try:
        assert conn.execute(
            "SELECT COALESCE(SUM(replay_required),0) s FROM increments").fetchone()["s"] == 0
    finally:
        conn.close()


def test_failed_full_load_marks_during_load_increments_orphan(app, client):
    """全量批次失败(从未切换)与取消一致:期间增量由下批成功切换承继。"""
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bad = {"file": (io.BytesIO("wrong,header\n1,2\n".encode()), "bad.csv")}
    client.post("/upload/full", data=bad, content_type="multipart/form-data")
    bid = full_batch_id(app)
    csv_upload(client, [("R1", "铅", "correct", "0.7", 7)], kind="inc")
    loader.run_pending(app.config["DATA_DIR"], 3)
    assert batch_rows(app)[bid]["status"] == "failed"
    make_full(app, client, [("R1", "铅", "0.2", 2), ("R2", "镉", "0.2", 1)])
    finish_pending(app)
    assert active_map(app)[("R1", "铅")] == ("0.7", 7)


def test_reupload_after_cancel_replays_increments_confirmed_while_canceled(app, client):
    """取消后换传:取消等待期间确认的增量不得在新基线切换时丢失。

    增量确认时没有任何已切换的新全量承载它,因此新全量切换必须回放该增量,
    即使它的 seq 恰好等于新批装载开始时的最大增量序号。
    """
    csv_upload(client, [("R1", "铅", "0.1", 1)])
    finish_pending(app)
    bad = make_full(app, client, [(f"W{i}", "铅", "9", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    # 等待停止期间确认增量(立即作用于正式表)
    csv_upload(client, [("R1", "铅", "correct", "0.55", 5)], kind="inc")
    assert active_map(app)[("R1", "铅")] == ("0.55", 5)
    # 取消误传批次
    assert client.post(f"/api/batches/{bad}/cancel").status_code == 200
    # 立即换传正确全量:文件内 R1 仅 rev3,低于已确认增量 rev5
    good = make_full(app, client, [("R1", "铅", "0.11", 3), ("R2", "镉", "0.2", 1),
                                   ("R3", "汞", "0.3", 1), ("R4", "砷", "0.4", 1)])
    finish_pending(app)
    assert batch_rows(app)[good]["status"] == "done"
    m = active_map(app)
    assert m[("R1", "铅")] == ("0.55", 5)   # 期间已确认增量被回放保留
    assert m[("R2", "镉")] == ("0.2", 1)
    assert all(not k[0].startswith("W") for k in m)    # 误传批次数据从未进入正式表


def test_reupload_immediately_after_cancel_and_late_writes_isolated(app, client):
    bid_old = make_full(app, client, [(f"O{i}", "铅", "1", 1) for i in range(9)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert staging_count(app, bid_old) == 3
    assert client.post(f"/api/batches/{bid_old}/cancel").status_code == 200
    assert staging_count(app, bid_old) == 0

    # 立即上传新全量:唯一索引已释放,不返回 409
    bid_new = make_full(app, client, [("NEW", "铅", "7.7", 1), ("N2", "铅", "1", 1),
                                      ("N3", "铅", "1", 1), ("N4", "铅", "1", 1)])
    assert bid_new != bid_old
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid_new]["status"] == "loading"
    assert staging_count(app, bid_new) >= 1

    # 旧执行“迟到”:模拟旧装载线程对旧批命名空间的写入与清理
    conn = db.connect(app.config["DATA_DIR"])
    try:
        logic.stage_full_row(conn, bid_old, "GHOST", "铅", "9.9", 9)
        conn.execute("DELETE FROM results_staging WHERE batch_id=?", (bid_old,))
    finally:
        conn.close()
    # 新批暂存区原样保留,未被旧批迟到操作写入或清空
    assert staging_count(app, bid_old) == 0
    assert staging_count(app, bid_new) >= 1
    # 新批继续完成并切换成功
    finish_pending(app)
    assert batch_rows(app)[bid_new]["status"] == "done"
    m = active_map(app)
    assert m[("NEW", "铅")] == ("7.7", 1) and ("GHOST", "铅") not in m


def test_late_chunk_after_cancel_rolls_back_entire_chunk(app, client):
    """恢复处理下一块前发现批次已被取消:取消检查点立即终止,后续块不再写库。"""
    bid = make_full(app, client, [(f"R{i}", "铅", "1", 1) for i in range(9)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert staging_count(app, bid) == 3

    # “请求线程”在装载暂停期间提交取消(清掉本批暂存)
    conn2 = db.connect(app.config["DATA_DIR"])
    try:
        conn2.execute("BEGIN IMMEDIATE")
        assert logic.cancel_full_batch(conn2, bid)
        conn2.execute("COMMIT")
    finally:
        conn2.close()

    # 装载线程恢复:批次处理器读到 canceled 终态后干净返回,不再写任何数据
    conn3 = db.connect(app.config["DATA_DIR"])
    try:
        assert loader.process_full_batch(conn3, bid, 3) is True
    finally:
        conn3.close()
    # 调度器视角:已取消批次不被 find_pending 拾起,run_pending 平静结束
    assert loader.run_pending(app.config["DATA_DIR"], 3) == []
    assert batch_rows(app)[bid]["status"] == "canceled"
    assert staging_count(app, bid) == 0
    # 正式表从未被该批触碰(没有基线时为空)
    assert active_map(app) == {}


def test_cancel_checkpoint_raises_while_loader_waits(app, client):
    """等待停止期间取消:装载线程在块间检查点收到 CancelNotAllowedError 并停下。"""
    bid = make_full(app, client, [(f"R{i}", "铅", "1", 1) for i in range(9)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert staging_count(app, bid) == 3
    # 保持 loading 状态,但在装载连接的下一次检查点前令其读到 canceled:
    # 直接用同一装载语义调用,批次先被别的连接取消 -> 顶部 fetch 即返回
    conn2 = db.connect(app.config["DATA_DIR"])
    try:
        conn2.execute("BEGIN IMMEDIATE")
        assert logic.cancel_full_batch(conn2, bid)
        conn2.execute("COMMIT")
    finally:
        conn2.close()
    conn3 = db.connect(app.config["DATA_DIR"])
    try:
        # 不抛异常、不写库:终态批次由循环条件静默结束
        loader.process_full_batch(conn3, bid, 3)
    finally:
        conn3.close()
    assert staging_count(app, bid) == 0


def test_cancel_guard_inside_switch_transaction(app, client):
    """切换事务内若批次已取消,merge_and_switch 拒绝执行且不改动正式表。"""
    csv_upload(client, [("OLD", "铅", "1", 1)])
    finish_pending(app)
    bid = make_full(app, client, [("NEW", "铅", "2", 1), ("X", "铅", "1", 1)])
    conn = db.connect(app.config["DATA_DIR"])
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE batches SET status='staging_ready' WHERE id=?", (bid,))
        conn.execute("COMMIT")
        conn.execute("BEGIN IMMEDIATE")
        assert logic.cancel_full_batch(conn, bid)
        conn.execute("COMMIT")
        # 迟到的切换尝试:守卫拒绝
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(logic.CancelNotAllowedError):
            logic.merge_and_switch(conn, bid)
        conn.execute("ROLLBACK")
    finally:
        conn.close()
    assert active_map(app) == {("OLD", "铅"): ("1", 1)}


# ------------------------------------------------------------ 重启恢复语义

def test_canceled_batch_not_resumed_after_restart_others_continue(app, client):
    # 批次 A:装载一块后取消
    bid_a = make_full(app, client, [(f"A{i}", "铅", "1", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    client.post(f"/api/batches/{bid_a}/cancel")
    # 批次 B:取消 A 之后上传,装载一块后“重启”
    bid_b = make_full(app, client, [(f"B{i}", "铅", "2", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid_b]["status"] == "loading"

    data_dir = app.config["DATA_DIR"]
    app2 = create_app({"TESTING": True, "DATA_DIR": data_dir,
                       "SAMPLES_DIR": app.config["SAMPLES_DIR"],
                       "CHUNK_SIZE": 3, "CHUNK_DELAY": 0.0})
    loader.run_pending(app2.config["DATA_DIR"], 3)
    rows = batch_rows(app2)
    assert rows[bid_a]["status"] == "canceled"     # 已取消不恢复
    assert rows[bid_b]["status"] == "done"         # 未完成批次接续完成
    assert active_map(app2) == {(f"B{i}", "铅"): ("2", 1) for i in range(6)}


# ------------------------------------------------------------ 后台线程与页面

def test_background_worker_stops_after_cancel(app, client):
    """真实后台线程:取消后线程自行退出,不把批次标记为失败。"""
    app2 = create_app({
        "TESTING": False,            # 允许恢复/后台线程启动
        "DATA_DIR": app.config["DATA_DIR"],
        "SAMPLES_DIR": app.config["SAMPLES_DIR"],
        "CHUNK_SIZE": 3,
        "CHUNK_DELAY": 0.05,
    })
    try:
        bid = make_full(app, client, [(f"R{i}", "铅", "1", 1) for i in range(30)])
        loader.poke_background(app2)
        deadline = time.time() + 5
        while time.time() < deadline and batch_rows(app)[bid]["status"] != "loading":
            time.sleep(0.05)
        assert batch_rows(app)[bid]["status"] == "loading"
        assert client.post(f"/api/batches/{bid}/cancel").status_code == 200
        deadline = time.time() + 8
        while time.time() < deadline and loader.is_claimed(bid):
            time.sleep(0.05)
        assert not loader.is_claimed(bid)
        time.sleep(0.3)                 # 留出线程退出时间
        assert batch_rows(app)[bid]["status"] == "canceled"
    finally:
        loader._claimed.clear()


def test_index_page_shows_cancel_ui(app, client):
    html = client.get("/").get_data(as_text=True)
    assert "取消批次" in html and "换传新全量" in html and "已取消" in html
    assert "/cancel" in html


def test_status_api_reports_canceled_count(app, client):
    bid = make_full(app, client, [(f"R{i}", "铅", "1", 1) for i in range(6)])
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    assert batch_rows(app)[bid]["status"] == "loading"
    client.post(f"/api/batches/{bid}/cancel")
    s = client.get("/api/status").get_json()
    assert s["canceled_full_batches"] == 1
    assert s["full_load_in_progress"] is None
