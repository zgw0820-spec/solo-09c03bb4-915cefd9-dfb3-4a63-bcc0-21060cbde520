#!/usr/bin/env python3
"""End-to-end self-test for the two interrupt points and recovery guarantees.

Runs the real FastAPI app in a uvicorn subprocess against the two local
PostgreSQL clusters managed by scripts/local_pg.py. The coordinator is
"killed" via its own os._exit(99) failpoints, then a fresh process is started
to exercise restart-and-continue - exactly the operator drill.

Checks:
  T1 failpoint after_both_prepared -> both DBs prepared, no decision ->
     restart -> recovery aborts; zero ledger rows; no residual prepared tx
  T2 failpoint after_one_commit -> debit committed, credit prepared,
     decision_commit logged -> restart -> recovery commits credit;
     exactly one ledger row per DB; no residual prepared tx
  T3 duplicate / concurrent recovery for the same settlement is rejected or
     converges without double posting (unique constraint + advisory lock)
  T4 cannot-verify case: prepared tx with no log evidence at all is held,
     not force-committed or force-rolled-back
  T5 normal happy path with no failpoint completes in one shot

Run:  .venv/bin/python -I scripts/self_test.py
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app import db as app_db  # noqa: E402

STATE = ROOT / ".local-pg" / "state.env"


def load_dsns() -> tuple[str, str]:
    env = {}
    for line in STATE.read_text().splitlines():
        k, v = line.split("=", 1)
        env[k] = v.strip().strip("'").strip('"')
    return env["DSN_DEBIT"], env["DSN_CREDIT"]


class WebApp:
    def __init__(self, dsn_debit: str, dsn_credit: str, failpoint: str):
        self.env = dict(os.environ)
        self.env.update({"DSN_DEBIT": dsn_debit, "DSN_CREDIT": dsn_credit,
                         "FAILPOINT": failpoint, "APP_PORT": "8080",
                         "PYTHONUNBUFFERED": "1"})
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        # Fail fast on a stale server holding the port, instead of waiting out
        # the whole health-check timeout.
        try:
            stale = httpx.get("http://127.0.0.1:8080/healthz", timeout=1)
            raise RuntimeError(
                f"port 8080 already answers (HTTP {stale.status_code}); "
                "stop the stale uvicorn before running the self-test")
        except httpx.HTTPError:
            pass
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", "8080", "--workers", "1"],
            cwd=str(ROOT), env=self.env,
            stdout=open(ROOT / ".local-pg" / "web.out.log", "ab"),
            stderr=open(ROOT / ".local-pg" / "web.err.log", "ab"))
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"web app exited during startup (rc={self.proc.returncode}); "
                    "port 8080 may already be in use")
            try:
                r = httpx.get("http://127.0.0.1:8080/healthz", timeout=2)
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        self.stop()
        raise RuntimeError("web app did not become healthy")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None

    def post_form(self, path: str, data: dict, timeout: float = 15):
        return httpx.post(f"http://127.0.0.1:8080{path}", data=data, timeout=timeout)

    def get_json(self, path: str):
        return httpx.get(f"http://127.0.0.1:8080{path}", timeout=10).json()


def prepared_gids(dsn: str) -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT gid FROM pg_prepared_xacts")
        return {r[0] for r in cur.fetchall()}


def ledger_count(dsn: str, settlement_no: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM ledger_entries WHERE settlement_no=%s",
                    (settlement_no,))
        return cur.fetchone()[0]


def ledger_rows(dsn: str, settlement_no: str) -> list[tuple[str, str]]:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute(
            "SELECT direction, amount::text FROM ledger_entries "
            "WHERE settlement_no=%s", (settlement_no,))
        return cur.fetchall()


def reversal_rows(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT original_no, reversal_no, status FROM reversals "
                    "ORDER BY id")
        return cur.fetchall()


def make_prepared_without_logs(dsn: str, gid: str, settlement_no: str) -> None:
    """Simulate an orphan prepared tx whose coordinator evidence is missing."""
    safe_gid = gid.replace("'", "''")
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO ledger_entries (settlement_no, direction, amount) "
            "VALUES (%s, 'DEBIT', 9.99)", (settlement_no,))
        cur.execute(f"PREPARE TRANSACTION '{safe_gid}'")


PASS, FAIL = 0, 0


def check(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name} {extra}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {extra}")


def main() -> int:
    dsn_debit, dsn_credit = load_dsns()
    # Make sure the application schema exists, then start from a clean slate.
    app_db.init_schema(dsn_debit)
    app_db.init_schema(dsn_credit)
    for dsn in (dsn_debit, dsn_credit):
        with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
            # Residual prepared transactions hold relation locks, so they must
            # be finalized (GID is a literal, not a bind parameter) before any
            # row-level cleanup that needs the same lock.
            cur.execute("SELECT gid FROM pg_prepared_xacts")
            for (gid,) in cur.fetchall():
                cur.execute("ROLLBACK PREPARED '" + gid.replace("'", "''") + "'")
            cur.execute("DELETE FROM xact_log")
            cur.execute("DELETE FROM ledger_entries")
            cur.execute("DELETE FROM reversals")

    # ---------- T1: crash after both prepared, no decision -> abort ------
    print("T1 两库已预备、尚无决定 (after_both_prepared)")
    no1 = "T1-ABORT-001"
    gid1 = f"settle_{no1}"
    app = WebApp(dsn_debit, dsn_credit, "after_both_prepared")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no1, "amount": "100.00"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("coordinator process exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("debit holds prepared tx", gid1 in prepared_gids(dsn_debit))
    check("credit holds prepared tx", gid1 in prepared_gids(dsn_credit))
    logs1 = [set(r["event"] for r in app_db.get_logs(d, no1))
             for d in (dsn_debit, dsn_credit)]
    check("no decision_commit/abort logged anywhere",
          all("decision_commit" not in s and "decision_abort" not in s for s in logs1),
          str(logs1))

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no1}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=aborted", r["result"]["status"] == "aborted")
        check("no residual prepared tx on debit", gid1 not in prepared_gids(dsn_debit))
        check("no residual prepared tx on credit", gid1 not in prepared_gids(dsn_credit))
        check("zero debit ledger rows", ledger_count(dsn_debit, no1) == 0)
        check("zero credit ledger rows", ledger_count(dsn_credit, no1) == 0)
        # Repeated recovery must be harmless.
        r2 = app2.post_form("/api/settlement/recover", {"settlement_no": no1}).json()
        check("repeat recovery -> noop (no double effect)",
              r2["result"]["status"] in ("noop", "aborted"))
        check("ledger counts still zero",
              ledger_count(dsn_debit, no1) == 0 and ledger_count(dsn_credit, no1) == 0)
    finally:
        app2.stop()

    # ---------- T2: crash after one commit -> finish commit --------------
    print("T2 仅一库已提交 (after_one_commit)")
    no2 = "T2-COMMIT-002"
    gid2 = f"settle_{no2}"
    app = WebApp(dsn_debit, dsn_credit, "after_one_commit")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no2, "amount": "250.50"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("coordinator process exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("debit prepared tx already gone (committed)",
          gid2 not in prepared_gids(dsn_debit))
    check("credit still holds prepared tx", gid2 in prepared_gids(dsn_credit))
    check("debit has exactly one ledger row", ledger_count(dsn_debit, no2) == 1)
    check("credit has no finalized ledger row yet", ledger_count(dsn_credit, no2) == 0)

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        view = app2.get_json(f"/api/settlement/{no2}")
        events = {r["event"] for r in view["debit_log"]} | {r["event"] for r in view["credit_log"]}
        check("decision_commit present in coordinator logs",
              "decision_commit" in events, str(sorted(events)))
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no2}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=committed", r["result"]["status"] == "committed")
        check("no residual prepared tx on debit", gid2 not in prepared_gids(dsn_debit))
        check("no residual prepared tx on credit", gid2 not in prepared_gids(dsn_credit))
        check("exactly one debit ledger row", ledger_count(dsn_debit, no2) == 1)
        check("exactly one credit ledger row", ledger_count(dsn_credit, no2) == 1)
        r2 = app2.post_form("/api/settlement/recover", {"settlement_no": no2}).json()
        check("repeat recovery converges, no double posting",
              r2["result"]["status"] == "committed"
              and ledger_count(dsn_debit, no2) == 1
              and ledger_count(dsn_credit, no2) == 1)
    finally:
        app2.stop()

    # ---------- T3: concurrent recovery of the same settlement ----------
    print("T3 同号并发恢复防护")
    no3 = "T3-CONC-003"
    gid3 = f"settle_{no3}"
    app = WebApp(dsn_debit, dsn_credit, "after_both_prepared")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no3, "amount": "7.00"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        results = []
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futs = [pool.submit(app2.post_form, "/api/settlement/recover",
                                {"settlement_no": no3}) for _ in range(5)]
            for f in concurrent.futures.as_completed(futs):
                results.append(f.result().json()["result"]["status"])
        print("       concurrent statuses:", results)
        check("exactly one recovery won, others rejected as duplicate",
              results.count("aborted") == 1 and results.count("duplicate") == 4,
              str(results))
        check("ledger never double-posted",
              ledger_count(dsn_debit, no3) == 0 and ledger_count(dsn_credit, no3) == 0)
        check("no prepared tx remains",
              gid3 not in prepared_gids(dsn_debit) and gid3 not in prepared_gids(dsn_credit))
    finally:
        app2.stop()

    # ---------- T4: cannot verify -> preserve prepared tx ---------------
    print("T4 无法核实日志 -> 保留事务并提示")
    no4 = "T4-HOLD-004"
    gid4 = f"settle_{no4}"
    # Orphan prepared tx on debit only; no xact_log rows anywhere.
    make_prepared_without_logs(dsn_debit, gid4, no4)
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no4}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=held", r["result"]["status"] == "held")
        check("prepared tx preserved on debit", gid4 in prepared_gids(dsn_debit))
        check("credit untouched", gid4 not in prepared_gids(dsn_credit))
        check("debit ledger row still inside preserved tx (not committed)",
              ledger_count(dsn_debit, no4) == 0)
    finally:
        app2.stop()
    # Cleanup the orphan so later reruns start clean.
    with psycopg.connect(dsn_debit, autocommit=True) as c, c.cursor() as cur:
        cur.execute("ROLLBACK PREPARED '" + gid4.replace("'", "''") + "'")

    # ---------- T5: happy path, no interrupt -----------------------------
    print("T5 无故障点的完整两阶段提交")
    no5 = "T5-HAPPY-005"
    gid5 = f"settle_{no5}"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        r = app.post_form("/api/settlement/start",
                          {"settlement_no": no5, "amount": "42.00"}).json()
        check("start ok", r.get("ok") is True, str(r))
        check("no prepared tx anywhere",
              gid5 not in prepared_gids(dsn_debit) and gid5 not in prepared_gids(dsn_credit))
        check("one ledger row per database",
              ledger_count(dsn_debit, no5) == 1 and ledger_count(dsn_credit, no5) == 1)
        # Restarting the same settlement number must be refused.
        try:
            dup = app.post_form("/api/settlement/start",
                                {"settlement_no": no5, "amount": "42.00"})
            check("duplicate start refused (409)", dup.status_code == 409)
        except Exception as exc:  # pragma: no cover
            check("duplicate start refused (409)", False, str(exc))
        check("ledger still exactly one per database",
              ledger_count(dsn_debit, no5) == 1 and ledger_count(dsn_credit, no5) == 1)
    finally:
        app.stop()

    # ---------- T6: happy-path reversal --------------------------------
    print("T6 全额冲正：等额反向流水、原流水保留、拒绝再次冲正")
    no6, rev6 = "T6-ORIG-006", "T6-REV-006"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no6, "amount": "88.80"}).json()
        r = app.post_form("/api/reversal/start",
                          {"original_no": no6, "reversal_no": rev6}).json()
        print("       reversal response:", json.dumps(r["result"], ensure_ascii=False))
        check("reversal status=committed",
              r.get("ok") is True and r["result"]["status"] == "committed", str(r))
        check("debit has original DEBIT and reversal CREDIT, equal amount",
              ledger_rows(dsn_debit, no6) == [("DEBIT", "88.80")] and
              ledger_rows(dsn_debit, rev6) == [("CREDIT", "88.80")],
              f"{ledger_rows(dsn_debit, no6)} / {ledger_rows(dsn_debit, rev6)}")
        check("credit has original CREDIT and reversal DEBIT, equal amount",
              ledger_rows(dsn_credit, no6) == [("CREDIT", "88.80")] and
              ledger_rows(dsn_credit, rev6) == [("DEBIT", "88.80")],
              f"{ledger_rows(dsn_credit, no6)} / {ledger_rows(dsn_credit, rev6)}")
        check("registry committed on both databases",
              reversal_rows(dsn_debit) == [(no6, rev6, "committed")] and
              reversal_rows(dsn_credit) == [(no6, rev6, "committed")])
        view = app.get_json(f"/api/reversal/{no6}")
        check("view links original to reversal and reports committed",
              [v["reversal_no"] for v in view["reversals"]] == [rev6]
              and view["reversals"][0]["status"] == "committed", str(view))
        # Second reversal of the same original must be refused, with the
        # already-successful reversal number available in the response view.
        r2 = app.post_form("/api/reversal/start",
                           {"original_no": no6, "reversal_no": "T6-REV-B"})
        check("second reversal refused (409)", r2.status_code == 409, r2.text[:160])
        check("refusal names the successful reversal",
              rev6 in r2.json().get("error", ""), r2.text[:200])
        # Reversal number cannot be taken by an ordinary settlement.
        dup = app.post_form("/api/settlement/start",
                            {"settlement_no": rev6, "amount": "1.00"})
        check("ordinary settlement cannot take a reversal number (409)",
              dup.status_code == 409, dup.text[:160])
        check("no extra reversal rows written",
              ledger_count(dsn_debit, "T6-REV-B") == 0
              and ledger_count(dsn_credit, "T6-REV-B") == 0)
    finally:
        app.stop()

    # ---------- T7: in-flight reversal is reported by number -----------
    print("T7 冲正中断后，重复办理时页面提示正在办理的冲正单号")
    no7, rev7 = "T7-ORIG-007", "T7-REV-007"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no7, "amount": "50.00"}).json()
    finally:
        app.stop()
    app = WebApp(dsn_debit, dsn_credit, "after_both_prepared")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/reversal/start",
                          {"original_no": no7, "reversal_no": rev7})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("reversal coordinator exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("reversal prepared tx held on both databases",
          f"revers_{rev7}" in prepared_gids(dsn_debit)
          and f"revers_{rev7}" in prepared_gids(dsn_credit))
    check("reversal number reserved before crash",
          (no7, rev7, "pending") in reversal_rows(dsn_debit)
          and (no7, rev7, "pending") in reversal_rows(dsn_credit),
          str(reversal_rows(dsn_debit)))

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        # A fresh reversal with a different number must point at the in-flight one.
        r = app2.post_form("/api/reversal/start",
                           {"original_no": no7, "reversal_no": "T7-REV-OTHER"})
        check("new reversal rejected (409) while one is in flight",
              r.status_code == 409, r.text[:160])
        check("rejection names the in-flight reversal number",
              rev7 in r.json().get("error", ""), r.text[:240])

        # Ordinary settlement must not take the reserved reversal number.
        s = app2.post_form("/api/settlement/start",
                           {"settlement_no": rev7, "amount": "5.00"})
        check("settlement cannot take in-flight reversal number (409)",
              s.status_code == 409, s.text[:160])
        # Recovering it via the settlement endpoint must be refused, too.
        s2 = app2.post_form("/api/settlement/recover", {"settlement_no": rev7})
        check("settlement recovery refuses a reversal number (409)",
              s2.status_code == 409, s2.text[:160])
    finally:
        app2.stop()

    # ---------- T8: crash during reversal, recover by reversal number ---
    print("T8 冲正中断 → 仅凭冲正单号从原入口恢复（两库均撤销）")
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/reversal/recover", {"reversal_no": rev7}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery by reversal number alone -> aborted",
              r["result"]["status"] == "aborted", str(r))
        check("original number resolved from registry",
              r["result"]["original_no"] == no7)
        check("no reversal prepared tx remains",
              f"revers_{rev7}" not in prepared_gids(dsn_debit)
              and f"revers_{rev7}" not in prepared_gids(dsn_credit))
        check("no reversal ledger row was committed",
              ledger_count(dsn_debit, rev7) == 0
              and ledger_count(dsn_credit, rev7) == 0)
        check("original ledger rows untouched by the aborted reversal",
              ledger_count(dsn_debit, no7) == 1
              and ledger_count(dsn_credit, no7) == 1)
        check("registry marked aborted in both databases",
              (no7, rev7, "aborted") in reversal_rows(dsn_debit)
              and (no7, rev7, "aborted") in reversal_rows(dsn_credit))
        # Aborted reversal number cannot be retried under the same number...
        r2 = app2.post_form("/api/reversal/start",
                            {"original_no": no7, "reversal_no": rev7})
        check("aborted reversal number cannot be reused (409)",
              r2.status_code == 409, r2.text[:160])
        # ...but a NEW number can retry the original.
        r3 = app2.post_form("/api/reversal/start",
                            {"original_no": no7, "reversal_no": "T7-REV-RETRY"}).json()
        check("retry with a new reversal number succeeds",
              r3.get("ok") is True and r3["result"]["status"] == "committed", str(r3))
        check("retry posted equal-and-opposite rows",
              ledger_rows(dsn_debit, "T7-REV-RETRY") == [("CREDIT", "50.00")]
              and ledger_rows(dsn_credit, "T7-REV-RETRY") == [("DEBIT", "50.00")])
    finally:
        app2.stop()

    # ---------- T9: concurrent reversals of one original ----------------
    print("T9 并发冲正：同一原单只允许一笔成功")
    no9 = "T9-ORIG-009"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no9, "amount": "33.00"}).json()
    finally:
        app.stop()
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        import concurrent.futures
        revs = [f"T9-REV-{i}" for i in range(6)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(app2.post_form, "/api/reversal/start",
                                {"original_no": no9, "reversal_no": rv})
                    for rv in revs]
            outcomes = [(f.result().status_code, f.result().json()) for f in
                        concurrent.futures.as_completed(futs)]
        oks = [body for code, body in outcomes if code == 200]
        refused = [body for code, body in outcomes if code == 409]
        print("       outcomes:", [(c, b.get('result', {}).get('status') or
                                    b.get('error')) for c, b in outcomes])
        check("exactly one concurrent reversal committed",
              len(oks) == 1 and oks[0]["result"]["status"] == "committed",
              str(len(oks)))
        check("all other concurrent attempts rejected (409)",
              len(refused) == 5 and len(outcomes) == 6)
        committed = [v for v in app2.get_json(f"/api/reversal/{no9}")["reversals"]
                     if v["status"] == "committed"]
        check("exactly one reversal row pair committed",
              len(committed) == 1, str(committed))
        check("only the winning reversal has ledger rows",
              sum(ledger_count(dsn_debit, rv) for rv in revs) == 1
              and sum(ledger_count(dsn_credit, rv) for rv in revs) == 1)
        check("original rows preserved exactly once",
              ledger_count(dsn_debit, no9) == 1 and ledger_count(dsn_credit, no9) == 1)
    finally:
        app2.stop()

    # ---------- T10: ineligible originals -------------------------------
    print("T10 冲正受理条件：缺流水、金额不一致、冲正单再冲正")
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        # Missing on one database.
        with psycopg.connect(dsn_debit, autocommit=True) as c, c.cursor() as cur:
            cur.execute(
                "INSERT INTO ledger_entries (settlement_no, direction, amount) "
                "VALUES ('T10-HALF', 'DEBIT', 10.00)")
        r = app2.post_form("/api/reversal/start",
                           {"original_no": "T10-HALF", "reversal_no": "T10-R1"})
        check("original missing in one database refused (409)",
              r.status_code == 409 and "两库" in r.json().get("error", ""),
              r.text[:200])
        check("refused attempt wrote nothing",
              not reversal_rows(dsn_debit) or all(
                  row[1] != "T10-R1" for row in reversal_rows(dsn_debit)))

        # Amount mismatch.
        with psycopg.connect(dsn_debit, autocommit=True) as c, c.cursor() as cur:
            cur.execute(
                "INSERT INTO ledger_entries (settlement_no, direction, amount) "
                "VALUES ('T10-DIFF', 'DEBIT', 10.00)")
        with psycopg.connect(dsn_credit, autocommit=True) as c, c.cursor() as cur:
            cur.execute(
                "INSERT INTO ledger_entries (settlement_no, direction, amount) "
                "VALUES ('T10-DIFF', 'CREDIT', 11.00)")
        r = app2.post_form("/api/reversal/start",
                           {"original_no": "T10-DIFF", "reversal_no": "T10-R2"})
        check("amount mismatch refused (409)",
              r.status_code == 409 and "金额不一致" in r.json().get("error", ""),
              r.text[:200])

        # Reversal of a reversal.
        r = app2.post_form("/api/reversal/start",
                           {"original_no": rev6, "reversal_no": "T10-R3"})
        check("reversing a reversal refused (409)",
              r.status_code == 409 and "冲正单" in r.json().get("error", ""),
              r.text[:200])
        # A reversal number already accepted for another original.
        app2.post_form("/api/settlement/start",
                       {"settlement_no": "T10-ORIG-010", "amount": "20.00"}).json()
        r = app2.post_form("/api/reversal/start",
                           {"original_no": "T10-ORIG-010",
                            "reversal_no": "T6-REV-006"})
        check("reversal number already accepted elsewhere refused (409)",
              r.status_code == 409 and "已被受理" in r.json().get("error", ""),
              r.text[:200])
        # A reversal number that is an existing settlement.
        r = app2.post_form("/api/reversal/start",
                           {"original_no": "T10-ORIG-010", "reversal_no": no6})
        check("reversal number equal to a completed settlement refused (409)",
              r.status_code == 409 and ("流水" in r.json().get("error", "")
                                        or "日志" in r.json().get("error", "")),
              r.text[:200])
        # Same number for original and reversal.
        r = app2.post_form("/api/reversal/start",
                           {"original_no": "T10-ORIG-010",
                            "reversal_no": "T10-ORIG-010"})
        check("original == reversal number refused (409)", r.status_code == 409,
              r.text[:200])
        # The rejected attempts must not have reserved anything or written rows.
        check("rejected attempts left the original untouched",
              ledger_count(dsn_debit, "T10-ORIG-010") == 1
              and ledger_count(dsn_credit, "T10-ORIG-010") == 1)
        check("no T10 rejection leaked a reversal registry row",
              all(row[0] != "T10-ORIG-010" for row in reversal_rows(dsn_debit)),
              str(reversal_rows(dsn_debit)))
    finally:
        app2.stop()

    # ---------- T11: reversal crash after one commit -> finish commit ---
    print("T11 冲正中断点二（一库已提交）→ 按冲正单号恢复补齐提交")
    no11, rev11 = "T11-ORIG-011", "T11-REV-011"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no11, "amount": "66.60"}).json()
    finally:
        app.stop()
    app = WebApp(dsn_debit, dsn_credit, "after_one_commit")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/reversal/start",
                          {"original_no": no11, "reversal_no": rev11})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("reversal coordinator exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("debit reversal row already committed",
          ledger_rows(dsn_debit, rev11) == [("CREDIT", "66.60")],
          str(ledger_rows(dsn_debit, rev11)))
    check("credit reversal still holds prepared tx",
          f"revers_{rev11}" in prepared_gids(dsn_credit))
    check("credit reversal row not committed yet",
          ledger_count(dsn_credit, rev11) == 0)

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/reversal/recover", {"reversal_no": rev11}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery by reversal number -> committed",
              r["result"]["status"] == "committed", str(r))
        check("credit reversal row now committed",
              ledger_rows(dsn_credit, rev11) == [("DEBIT", "66.60")],
              str(ledger_rows(dsn_credit, rev11)))
        check("no residual prepared tx",
              f"revers_{rev11}" not in prepared_gids(dsn_debit)
              and f"revers_{rev11}" not in prepared_gids(dsn_credit))
        check("original rows preserved",
              ledger_rows(dsn_debit, no11) == [("DEBIT", "66.60")]
              and ledger_rows(dsn_credit, no11) == [("CREDIT", "66.60")])
        check("registry committed in both databases",
              (no11, rev11, "committed") in reversal_rows(dsn_debit)
              and (no11, rev11, "committed") in reversal_rows(dsn_credit))
        # Repeat recovery must converge without double posting.
        r2 = app2.post_form("/api/reversal/recover", {"reversal_no": rev11}).json()
        check("repeat recovery converges",
              r2["result"]["status"] == "committed"
              and ledger_count(dsn_debit, rev11) == 1
              and ledger_count(dsn_credit, rev11) == 1)
    finally:
        app2.stop()

    # ---------- T12: crash between the two registry reservations --------
    print("T12 仅登记冲正号后崩溃 → 恢复确认撤销 → 换号重试")
    no12, rev12 = "T12-ORIG-012", "T12-REV-012"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no12, "amount": "12.00"}).json()
        # Simulate the crash window: the reversal number is registered in both
        # databases but no business transaction was ever prepared.
        app_db.reserve_reversal(dsn_debit, no12, rev12)
        app_db.reserve_reversal(dsn_credit, no12, rev12)
        # A same-number resubmission must NOT quietly retry: it has to be
        # confirmed aborted first.
        r = app.post_form("/api/reversal/start",
                          {"original_no": no12, "reversal_no": rev12})
        check("reserved-only reversal cannot be retried in place (409)",
              r.status_code == 409 and "恢复" in r.json().get("error", ""),
              r.text[:200])
        # A different number must point at the reserved one.
        r = app.post_form("/api/reversal/start",
                          {"original_no": no12, "reversal_no": "T12-REV-OTHER"})
        check("other number rejected while one is reserved (409)",
              r.status_code == 409 and rev12 in r.json().get("error", ""),
              r.text[:240])
        check("reserved reversal did not post anything",
              ledger_count(dsn_debit, rev12) == 0
              and ledger_count(dsn_credit, rev12) == 0)
        # Recovery confirms "nothing was written" in both databases.
        r = app.post_form("/api/reversal/recover", {"reversal_no": rev12}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery confirms abort of a reserved-only reversal",
              r["result"]["status"] == "aborted", str(r))
        check("registry marked aborted in both databases",
              (no12, rev12, "aborted") in reversal_rows(dsn_debit)
              and (no12, rev12, "aborted") in reversal_rows(dsn_credit))
        # Now the original may be retried with a new number.
        r = app.post_form("/api/reversal/start",
                          {"original_no": no12, "reversal_no": "T12-REV-RETRY"}).json()
        check("retry after confirmed abort succeeds",
              r.get("ok") is True and r["result"]["status"] == "committed", str(r))
        check("retry posted equal-and-opposite rows",
              ledger_rows(dsn_debit, "T12-REV-RETRY") == [("CREDIT", "12.00")]
              and ledger_rows(dsn_credit, "T12-REV-RETRY") == [("DEBIT", "12.00")])
    finally:
        app.stop()

    # ---------- T13: crash between the two PREPARE calls ----------------
    print("T13 冲正仅一库已 PREPARE 即崩溃 → 恢复撤销 → 换号重试")
    no13, rev13 = "T13-ORIG-013", "T13-REV-013"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        app.post_form("/api/settlement/start",
                      {"settlement_no": no13, "amount": "15.00"}).json()
        # Simulate the crash window: reversal number registered in both DBs,
        # business tx prepared in the debit DB only, no log row anywhere.
        app_db.reserve_reversal(dsn_debit, no13, rev13)
        app_db.reserve_reversal(dsn_credit, no13, rev13)
        make_prepared_without_logs(dsn_debit, f"revers_{rev13}", rev13)
        r = app.post_form("/api/reversal/recover", {"reversal_no": rev13}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("single-sided prepare recovery -> aborted",
              r["result"]["status"] == "aborted", str(r))
        check("prepared tx rolled back on debit",
              f"revers_{rev13}" not in prepared_gids(dsn_debit))
        check("nothing posted for the reversal",
              ledger_count(dsn_debit, rev13) == 0 and ledger_count(dsn_credit, rev13) == 0)
        check("original rows untouched",
              ledger_count(dsn_debit, no13) == 1 and ledger_count(dsn_credit, no13) == 1)
        check("registry aborted in both databases",
              (no13, rev13, "aborted") in reversal_rows(dsn_debit)
              and (no13, rev13, "aborted") in reversal_rows(dsn_credit))
        r = app.post_form("/api/reversal/start",
                          {"original_no": no13, "reversal_no": "T13-REV-RETRY"}).json()
        check("retry with a new number succeeds after single-sided abort",
              r.get("ok") is True and r["result"]["status"] == "committed", str(r))
        check("retry posted equal-and-opposite rows",
              ledger_rows(dsn_debit, "T13-REV-RETRY") == [("CREDIT", "15.00")]
              and ledger_rows(dsn_credit, "T13-REV-RETRY") == [("DEBIT", "15.00")])
    finally:
        app.stop()

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
