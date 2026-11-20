"""Two-phase settlement coordinator with crash injection and recovery.

Lifecycle of one settlement (GID = ``settle_<settlement_no>`` in both DBs):

  1. each DB prepares its business write with PREPARE TRANSACTION
  2. each DB records event 'prepared' in xact_log (separate committed tx)
  3. the decision is written to xact_log of BOTH databases
     ('decision_commit' or 'decision_abort')
  4. the prepared transactions are finalized (COMMIT/ROLLBACK PREPARED)
  5. each DB records the terminal event ('committed' / 'aborted')

Injected failure points (FAILPOINT env var):

  after_both_prepared - hard process exit once both DBs have prepared but
                        before any decision row exists ("no decision yet")
  after_one_commit    - hard process exit after the debit DB has committed
                        its prepared transaction but before the credit DB

Recovery rules:

  * decision_commit visible in either log  -> finish COMMIT on both DBs
  * decision_abort  visible in either log  -> finish ROLLBACK on both DBs
  * no decision row anywhere and both prepared -> ABORT
  * no decision row and one side already finalized (commit/rollback cannot
    be told apart from the log) and the other still prepared -> KEEP both
    prepared transactions in place and report "cannot verify"
  * terminal status is only reported once neither DB still holds the
    prepared transaction; otherwise the transaction is preserved and the
    operator is told to intervene.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from . import db
from .config import (
    FAIL_AFTER_BOTH_PREPARED,
    FAIL_AFTER_ONE_COMMIT,
    gid_for,
    gid_for_reversal,
    validate_settlement_no,
)


@dataclass
class RecoveryResult:
    settlement_no: str
    status: str                       # committed | aborted | held | noop | duplicate
    decision_basis: str               # human-readable evidence description
    detail: dict[str, Any] = field(default_factory=dict)


class InconsistentSettlement(RuntimeError):
    """The two databases disagree in a way that needs a human on-call."""


# ---------------------------------------------------------------------------
# Reversal (全额冲正) state machine
#
# A reversal is itself a two-phase transaction, but its GID (revers_<no>) and
# ledger key (the reversal number) are distinct from the original settlement,
# so both the original row and its equal-and-opposite reversal row coexist.
#
# The reversal number is reserved in the ``reversals`` registry of BOTH
# databases before any business write is prepared. That reservation is what
# makes a number "accepted as a reversal", so an ordinary settlement can never
# grab it.
# ---------------------------------------------------------------------------

REVERSAL_COMMITTED = "committed"
REVERSAL_ABORTED = "aborted"
REVERSAL_IN_PROGRESS = "in_progress"
REVERSAL_INCONSISTENT = "inconsistent"
REVERSAL_RESERVED = "reserved"


class ReversalRejected(RuntimeError):
    """The requested reversal is not acceptable; the message is operator-facing."""


@dataclass
class ReversalResult:
    original_no: str
    reversal_no: str
    status: str                       # committed | aborted | held | duplicate | rejected
    basis: str
    detail: dict[str, Any] = field(default_factory=dict)


def _reversal_gid(reversal_no: str) -> str:
    return gid_for_reversal(reversal_no)


def _events(dsn: str, key: str) -> set[str]:
    return {row["event"] for row in db.get_logs(dsn, key)}


def _reversal_evidence(dsn_debit: str, dsn_credit: str,
                       original_no: str, reversal_no: str) -> dict[str, Any]:
    """Collect everything needed to classify one reversal attempt."""
    gid = _reversal_gid(reversal_no)
    debit_registry = db.get_reversal(dsn_debit, reversal_no)
    credit_registry = db.get_reversal(dsn_credit, reversal_no)
    registry = debit_registry or credit_registry
    return {
        "original_no": original_no,
        "reversal_no": reversal_no,
        "debit_registry": debit_registry,
        "credit_registry": credit_registry,
        "registry_status": registry["status"] if registry else None,
        "debit_ledger": db.get_ledger_entry(dsn_debit, reversal_no),
        "credit_ledger": db.get_ledger_entry(dsn_credit, reversal_no),
        "debit_prepared": db.prepared_exists(dsn_debit, gid),
        "credit_prepared": db.prepared_exists(dsn_credit, gid),
        "debit_events": _events(dsn_debit, reversal_no),
        "credit_events": _events(dsn_credit, reversal_no),
    }


def _classify_reversal(ev: dict[str, Any]) -> str:
    """Derive the attempt state from database evidence, not just the registry.

    * both reversal ledger rows       -> committed
    * one row or any prepared tx      -> in_progress (recovery can settle it)
    * no row, no prepared tx:
        - decision_commit somewhere   -> inconsistent (needs a human)
        - abort confirmed in BOTH     -> aborted (safe to retry with a new no)
        - any other log evidence      -> in_progress (run recovery first)
        - nothing at all              -> reserved (accepted, no business write;
                                         recovery confirms the abort first)
    """
    debit_row = ev["debit_ledger"]
    credit_row = ev["credit_ledger"]
    if debit_row and credit_row:
        return REVERSAL_COMMITTED
    if debit_row or credit_row:
        return REVERSAL_IN_PROGRESS
    if ev["debit_prepared"] or ev["credit_prepared"]:
        return REVERSAL_IN_PROGRESS

    all_events = ev["debit_events"] | ev["credit_events"]
    if db.EVENT_DECISION_COMMIT in all_events:
        return REVERSAL_INCONSISTENT
    abort_confirmed = (
        (db.EVENT_DECISION_ABORT in ev["debit_events"]
         and db.EVENT_DECISION_ABORT in ev["credit_events"])
        or (db.EVENT_ABORTED in ev["debit_events"]
            and db.EVENT_ABORTED in ev["credit_events"])
    )
    if abort_confirmed:
        return REVERSAL_ABORTED
    if all_events:
        return REVERSAL_IN_PROGRESS

    # No business evidence at all. The registry is the only remaining source.
    debit_registry = ev["debit_registry"]
    credit_registry = ev["credit_registry"]
    if (debit_registry and credit_registry
            and debit_registry["status"] == db.REVERSAL_ABORTED
            and credit_registry["status"] == db.REVERSAL_ABORTED):
        return REVERSAL_ABORTED
    if (debit_registry and debit_registry["status"] == db.REVERSAL_COMMITTED) \
            or (credit_registry and credit_registry["status"] == db.REVERSAL_COMMITTED):
        return REVERSAL_INCONSISTENT
    return REVERSAL_RESERVED


def _known_reversal_numbers(dsn_debit: str, dsn_credit: str,
                            original_no: str) -> set[str]:
    numbers = {row["reversal_no"]
               for row in db.get_reversals_for_original(dsn_debit, original_no)}
    numbers |= {row["reversal_no"]
                for row in db.get_reversals_for_original(dsn_credit, original_no)}
    return numbers


def _reconcile_status(dsn_debit: str, dsn_credit: str, original_no: str,
                      reversal_no: str, status: str) -> None:
    """Best-effort registry update on both databases (idempotent)."""
    for dsn in (dsn_debit, dsn_credit):
        row = db.get_reversal(dsn, reversal_no)
        if row is None:
            db.reserve_reversal(dsn, original_no, reversal_no)
        if row is None or row["status"] != status:
            db.set_reversal_status(dsn, reversal_no, status)


def _validate_original(dsn_debit: str, dsn_credit: str,
                       original_no: str) -> Decimal:
    """Return the original amount, or raise ReversalRejected.

    Only an ordinary settlement that has already committed a ledger row in
    BOTH databases, with identical amounts, is eligible.
    """
    if db.reversal_number_exists(dsn_debit, original_no) \
            or db.reversal_number_exists(dsn_credit, original_no):
        raise ReversalRejected("该单号是冲正单，不能对冲正单再次冲正")

    orig_debit = db.get_ledger_entry(dsn_debit, original_no)
    orig_credit = db.get_ledger_entry(dsn_credit, original_no)
    if orig_debit is None or orig_credit is None:
        raise ReversalRejected(
            "两库必须都存在该原结算的已提交流水，才可办理冲正")
    if orig_debit["direction"] != "DEBIT" or orig_credit["direction"] != "CREDIT":
        raise ReversalRejected("只受理普通结算的冲正（扣款库应为 DEBIT、入账库应为 CREDIT）")
    if Decimal(str(orig_debit["amount"])) != Decimal(str(orig_credit["amount"])):
        raise ReversalRejected(
            f"两库原流水金额不一致（debit={orig_debit['amount']}, "
            f"credit={orig_credit['amount']}），不能冲正")
    return Decimal(str(orig_debit["amount"]))


def _ensure_reversal_number_free(dsn_debit: str, dsn_credit: str,
                                 original_no: str, reversal_no: str) -> None:
    """A reversal number must not already be used as a settlement anywhere."""
    for name, dsn in (("debit", dsn_debit), ("credit", dsn_credit)):
        row = db.get_reversal(dsn, reversal_no)
        if row is not None and row["original_no"] != original_no:
            raise ReversalRejected(
                f"冲正单号 {reversal_no} 已被受理为原单 {row['original_no']} 的冲正")
        if db.prepared_exists(dsn, gid_for(reversal_no)):
            raise ReversalRejected(f"冲正单号 {reversal_no} 在 {name} 库已有预备事务")
        if db.get_ledger_entry(dsn, reversal_no) is not None:
            raise ReversalRejected(f"冲正单号 {reversal_no} 在 {name} 库已有流水")
        if db.get_logs(dsn, reversal_no):
            raise ReversalRejected(f"冲正单号 {reversal_no} 在 {name} 库已有协调日志")


def list_reversals_view(dsn_debit: str, dsn_credit: str,
                        original_no: str) -> list[dict[str, Any]]:
    """Everything the ops page needs about one original settlement's reversals."""
    view = []
    for reversal_no in sorted(_known_reversal_numbers(dsn_debit, dsn_credit, original_no)):
        ev = _reversal_evidence(dsn_debit, dsn_credit, original_no, reversal_no)
        state = _classify_reversal(ev)
        registry = (db.get_reversal(dsn_debit, reversal_no)
                    or db.get_reversal(dsn_credit, reversal_no))
        view.append({
            "reversal_no": reversal_no,
            "original_no": original_no,
            "status": state,
            "registry_status": registry["status"] if registry else None,
            "debit_ledger": ev["debit_ledger"],
            "credit_ledger": ev["credit_ledger"],
            "debit_prepared": ev["debit_prepared"],
            "credit_prepared": ev["credit_prepared"],
            "debit_log": db.get_logs(dsn_debit, reversal_no),
            "credit_log": db.get_logs(dsn_credit, reversal_no),
        })
    return view


def start_reversal(dsn_debit: str, dsn_credit: str,
                   original_no: str, reversal_no: str,
                   failpoint: str) -> ReversalResult:
    """Run (or crash inside) a full equal-and-opposite reversal.

    Raises ReversalRejected for any business-rule violation. The whole
    check-accept-prepare-commit sequence holds an advisory lock keyed by the
    ORIGINAL number, so two attempts on the same original cannot both succeed.
    """
    original_no = validate_settlement_no(original_no)
    reversal_no = validate_settlement_no(reversal_no)
    if original_no == reversal_no:
        raise ReversalRejected("原结算号与冲正单号不能相同")

    with db.reversal_lock(dsn_debit, original_no) as acquired:
        if not acquired:
            known = sorted(_known_reversal_numbers(dsn_debit, dsn_credit, original_no))
            hint = f"（冲正单号 {known[0]}）" if known else ""
            raise ReversalRejected(
                f"该原结算的冲正正在办理中{hint}，请勿重复提交")

        amount = _validate_original(dsn_debit, dsn_credit, original_no)

        # Inspect every prior attempt. A successful reversal blocks forever;
        # an attempt still in flight blocks and is echoed back by number; only
        # a fully aborted attempt permits retrying with a NEW number.
        for prior_no in sorted(_known_reversal_numbers(dsn_debit, dsn_credit,
                                                       original_no)):
            ev = _reversal_evidence(dsn_debit, dsn_credit, original_no, prior_no)
            state = _classify_reversal(ev)
            if prior_no == reversal_no:
                if state == REVERSAL_COMMITTED:
                    raise ReversalRejected(
                        f"原单 {original_no} 已由冲正单 {reversal_no} 冲正成功，不能重复冲正")
                if state == REVERSAL_ABORTED:
                    raise ReversalRejected(
                        f"冲正单 {reversal_no} 已撤销；如需重试请换用新的冲正单号")
                if state == REVERSAL_RESERVED:
                    raise ReversalRejected(
                        f"冲正单 {reversal_no} 已受理但未写入任何业务流水；"
                        "请先按冲正单号恢复确认撤销，再换新号重试")
                raise ReversalRejected(
                    f"冲正单 {reversal_no} 正在办理中，请先按冲正单号恢复该冲正单")
            if state == REVERSAL_COMMITTED:
                raise ReversalRejected(
                    f"原单 {original_no} 已由冲正单 {prior_no} 冲正成功，不能重复冲正")
            if state == REVERSAL_ABORTED:
                _reconcile_status(dsn_debit, dsn_credit, original_no,
                                  prior_no, db.REVERSAL_ABORTED)
                continue
            hint = (f"冲正单 {prior_no} 仅登记未写入业务流水，请先按该冲正单号恢复确认"
                    if state == REVERSAL_RESERVED
                    else f"请按该冲正单从原入口恢复")
            raise ReversalRejected(
                f"原单 {original_no} 已有冲正单 {prior_no} 在办理中，{hint}")

        _ensure_reversal_number_free(dsn_debit, dsn_credit, original_no, reversal_no)

        with db.number_lock(dsn_debit, reversal_no) as number_ok:
            if not number_ok:
                raise ReversalRejected(f"冲正单号 {reversal_no} 正在被并发使用")
            # Re-check under the lock: an ordinary settlement of this number
            # may have started between the check above and here.
            _ensure_reversal_number_free(dsn_debit, dsn_credit,
                                         original_no, reversal_no)
            return _drive_reversal(dsn_debit, dsn_credit, original_no,
                                   reversal_no, amount, failpoint)


def _drive_reversal(dsn_debit: str, dsn_credit: str, original_no: str,
                    reversal_no: str, amount: Decimal, failpoint: str) -> ReversalResult:
    gid = _reversal_gid(reversal_no)

    # ---- Reserve the number in BOTH registries before writing business rows.
    # If the process dies between the two reservations, the next attempt (or
    # recovery) sees a "reserved, nothing written" attempt and can confirm the
    # abort, so a half reservation can never wedge the original permanently.
    db.reserve_reversal(dsn_debit, original_no, reversal_no)
    db.reserve_reversal(dsn_credit, original_no, reversal_no)

    # ---- Phase 1: prepare equal-and-opposite rows in both databases --------
    # Debit DB: money goes back (CREDIT). Credit DB: money is taken back (DEBIT).
    db.prepare_business_tx(dsn_debit, reversal_no, "CREDIT", amount, gid)
    db.prepare_business_tx(dsn_credit, reversal_no, "DEBIT", amount, gid)
    db.log_event(dsn_debit, reversal_no, db.EVENT_PREPARED,
                 f"reversal of {original_no} prepared")
    db.log_event(dsn_credit, reversal_no, db.EVENT_PREPARED,
                 f"reversal of {original_no} prepared")

    if failpoint == FAIL_AFTER_BOTH_PREPARED:
        _hard_exit(f"reversal {reversal_no} after_both_prepared")

    # ---- Phase 2: durable commit decision on BOTH databases ----------------
    db.log_event(dsn_debit, reversal_no, db.EVENT_DECISION_COMMIT,
                 f"reversal of {original_no}: decision commit")
    db.log_event(dsn_credit, reversal_no, db.EVENT_DECISION_COMMIT,
                 f"reversal of {original_no}: decision commit")

    # ---- Phase 3: finalize -------------------------------------------------
    debit_outcome = db.commit_prepared(dsn_debit, gid)
    db.log_event(dsn_debit, reversal_no, db.EVENT_COMMITTED,
                 f"reversal COMMIT PREPARED result={debit_outcome}")

    if failpoint == FAIL_AFTER_ONE_COMMIT:
        _hard_exit(f"reversal {reversal_no} after_one_commit")

    credit_outcome = db.commit_prepared(dsn_credit, gid)
    db.log_event(dsn_credit, reversal_no, db.EVENT_COMMITTED,
                 f"reversal COMMIT PREPARED result={credit_outcome}")

    _reconcile_status(dsn_debit, dsn_credit, original_no,
                      reversal_no, db.REVERSAL_COMMITTED)
    return ReversalResult(
        original_no, reversal_no, "committed",
        "reversal committed in both databases; original row preserved",
        {"amount": str(amount),
         "debit": {"direction": "CREDIT", "finalize": debit_outcome},
         "credit": {"direction": "DEBIT", "finalize": credit_outcome}})


def recover_reversal(dsn_debit: str, dsn_credit: str,
                     original_no: str | None, reversal_no: str) -> ReversalResult:
    """Resume a crashed reversal, entered by reversal number.

    ``original_no`` may be omitted: the registry row (written before any
    business write) is looked up in both databases. This is what lets the
    coordinator restart and recover "from the original entry point" using
    only the reversal number.

    Mirrors settlement recovery: a durable commit decision wins; a durable
    abort decision rolls back; no decision with both still prepared rolls back;
    anything ambiguous is held untouched. Registry rows are only flipped after
    both databases have reached a terminal state.
    """
    reversal_no = validate_settlement_no(reversal_no)
    if original_no:
        original_no = validate_settlement_no(original_no)
    registry = (db.get_reversal(dsn_debit, reversal_no)
                or db.get_reversal(dsn_credit, reversal_no))
    if original_no:
        if registry is not None and registry["original_no"] != original_no:
            raise ReversalRejected(
                f"冲正单 {reversal_no} 登记的原单是 {registry['original_no']}，"
                f"与填写的 {original_no} 不一致")
    else:
        if registry is None:
            raise ReversalRejected(
                f"冲正单号 {reversal_no} 未被受理，无法按冲正单恢复")
        original_no = registry["original_no"]

    with db.reversal_lock(dsn_debit, original_no) as acquired:
        if not acquired:
            return ReversalResult(
                original_no, reversal_no, "duplicate",
                "another operation for this original settlement is already running")

        ev = _reversal_evidence(dsn_debit, dsn_credit, original_no, reversal_no)
        state = _classify_reversal(ev)
        if state == REVERSAL_COMMITTED:
            _reconcile_status(dsn_debit, dsn_credit, original_no,
                              reversal_no, db.REVERSAL_COMMITTED)
            return ReversalResult(
                original_no, reversal_no, "committed",
                "both databases already hold the reversal ledger rows",
                {"debit_prepared": False, "credit_prepared": False})
        if state == REVERSAL_ABORTED:
            _reconcile_status(dsn_debit, dsn_credit, original_no,
                              reversal_no, db.REVERSAL_ABORTED)
            return ReversalResult(
                original_no, reversal_no, "aborted",
                "no prepared transaction and no reversal row; abort confirmed in both "
                "databases — the original may be retried with a new reversal number",
                {"debit_prepared": False, "credit_prepared": False})
        if state == REVERSAL_INCONSISTENT:
            return ReversalResult(
                original_no, reversal_no, "held",
                "a commit decision exists but the reversal ledger rows are missing "
                "in at least one database; human investigation required",
                {"debit_ledger": ev["debit_ledger"],
                 "credit_ledger": ev["credit_ledger"]})
        if state == REVERSAL_RESERVED:
            # The number was reserved before any business write, and neither a
            # prepared transaction nor a log row exists: nothing was posted.
            # Record the confirmation in both databases' coordinator logs, then
            # treat the attempt as aborted and allow retry with a new number.
            for dsn in (dsn_debit, dsn_credit):
                db.log_event(dsn, reversal_no, db.EVENT_DECISION_ABORT,
                             "reversal reservation confirmed unused during recovery")
                db.log_event(dsn, reversal_no, db.EVENT_ABORTED,
                             "no business transaction was ever prepared")
            _reconcile_status(dsn_debit, dsn_credit, original_no,
                              reversal_no, db.REVERSAL_ABORTED)
            return ReversalResult(
                original_no, reversal_no, "aborted",
                "only the reversal number was reserved; no business transaction "
                "was ever prepared in either database — the original may be "
                "retried with a new reversal number",
                {"debit_prepared": False, "credit_prepared": False})

        debit_prepared = ev["debit_prepared"]
        credit_prepared = ev["credit_prepared"]
        gid = _reversal_gid(reversal_no)

        if not debit_prepared and not credit_prepared:
            return ReversalResult(
                original_no, reversal_no, "held",
                "prepared transactions are gone but abort is not confirmed in both "
                "databases; human investigation required",
                {"debit_events": sorted(ev["debit_events"]),
                 "credit_events": sorted(ev["credit_events"])})

        # A crash between the two PREPARE calls leaves one side prepared and
        # the other with no evidence at all. There is no commit decision, so
        # rolling the lone prepared transaction back is always safe: nothing
        # can have been committed (a commit decision would have been logged
        # first and written to both databases). Without this the original
        # would be wedged in "held" forever.
        one_sided_no_decision = (
            debit_prepared != credit_prepared
            and db.EVENT_DECISION_COMMIT not in (ev["debit_events"]
                                                 | ev["credit_events"])
            and db.EVENT_DECISION_ABORT not in (ev["debit_events"]
                                                | ev["credit_events"]))
        if one_sided_no_decision:
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_ABORT not in _events(dsn, reversal_no):
                    db.log_event(dsn, reversal_no, db.EVENT_DECISION_ABORT,
                                 "single-sided prepare aborted during recovery")
            outcomes: dict[str, str] = {}
            if debit_prepared:
                outcomes["debit"] = db.rollback_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, reversal_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.rollback_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, reversal_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['credit']}")
            if db.prepared_exists(dsn_debit, gid) or db.prepared_exists(dsn_credit, gid):
                return ReversalResult(
                    original_no, reversal_no, "held",
                    "finalization incomplete; prepared transaction preserved for retry")
            _reconcile_status(dsn_debit, dsn_credit, original_no,
                              reversal_no, db.REVERSAL_ABORTED)
            return ReversalResult(
                original_no, reversal_no, "aborted",
                "only one database had prepared the reversal and no decision was "
                "recorded; rolled back safely — retry with a new reversal number",
                {"finalize": outcomes,
                 "debit_prepared": False, "credit_prepared": False})

        decision, basis = _decide(dsn_debit, dsn_credit, reversal_no,
                                  debit_prepared, credit_prepared)
        if decision == "hold":
            return ReversalResult(
                original_no, reversal_no, "held", basis,
                {"debit_prepared": debit_prepared,
                 "credit_prepared": credit_prepared})

        outcomes: dict[str, str] = {}
        if decision == "commit":
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_COMMIT not in _events(dsn, reversal_no):
                    db.log_event(dsn, reversal_no, db.EVENT_DECISION_COMMIT,
                                 "reversal decision replicated during recovery")
            if debit_prepared:
                outcomes["debit"] = db.commit_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, reversal_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.commit_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, reversal_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['credit']}")
        else:
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_ABORT not in _events(dsn, reversal_no):
                    db.log_event(dsn, reversal_no, db.EVENT_DECISION_ABORT,
                                 "reversal abort decision recorded during recovery")
            if debit_prepared:
                outcomes["debit"] = db.rollback_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, reversal_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.rollback_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, reversal_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['credit']}")

        still_debit = db.prepared_exists(dsn_debit, gid)
        still_credit = db.prepared_exists(dsn_credit, gid)
        if still_debit or still_credit:
            return ReversalResult(
                original_no, reversal_no, "held",
                "finalization incomplete; prepared transaction preserved for retry",
                {"debit_prepared": still_debit, "credit_prepared": still_credit})

        # Verify the business outcome before publishing a terminal status.
        debit_row = db.get_ledger_entry(dsn_debit, reversal_no)
        credit_row = db.get_ledger_entry(dsn_credit, reversal_no)
        if decision == "commit":
            if not (debit_row and credit_row):
                return ReversalResult(
                    original_no, reversal_no, "held",
                    "commit finalization reported success but a reversal row is "
                    "missing; human investigation required",
                    {"debit_ledger": debit_row, "credit_ledger": credit_row})
            _reconcile_status(dsn_debit, dsn_credit, original_no,
                              reversal_no, db.REVERSAL_COMMITTED)
            return ReversalResult(
                original_no, reversal_no, "committed", basis,
                {"finalize": outcomes,
                 "debit_ledger": debit_row, "credit_ledger": credit_row})

        if debit_row or credit_row:
            return ReversalResult(
                original_no, reversal_no, "held",
                "abort finalization left a reversal ledger row; human investigation "
                "required",
                {"debit_ledger": debit_row, "credit_ledger": credit_row})
        _reconcile_status(dsn_debit, dsn_credit, original_no,
                          reversal_no, db.REVERSAL_ABORTED)
        return ReversalResult(
            original_no, reversal_no, "aborted", basis,
            {"finalize": outcomes, "debit_prepared": False, "credit_prepared": False})



def _hard_exit(reason: str) -> None:
    os.write(2, f"[coordinator] INJECTED CRASH at {reason}, exiting\n".encode())
    os._exit(99)


def start_settlement(dsn_debit: str, dsn_credit: str,
                     settlement_no: str, amount: float,
                     failpoint: str) -> None:
    """Run (or crash inside) the two-phase settlement."""
    settlement_no = validate_settlement_no(settlement_no)
    gid = gid_for(settlement_no)

    # Idempotency guard: a fully or partially processed settlement must not
    # be driven through prepare a second time (that would double-post).
    for name, dsn in (("debit", dsn_debit), ("credit", dsn_credit)):
        if db.prepared_exists(dsn, gid) or db.has_ledger_entry(dsn, settlement_no):
            raise InconsistentSettlement(
                f"settlement {settlement_no} already exists in {name} database; "
                "use recovery instead of starting it again")
        # A number that was accepted as a reversal belongs to that reversal and
        # must never be reused for an ordinary settlement.
        if db.reversal_number_exists(dsn, settlement_no):
            raise InconsistentSettlement(
                f"{settlement_no} is already accepted as a reversal number; "
                "an ordinary settlement cannot take it")

    with db.number_lock(dsn_debit, settlement_no) as number_ok:
        if not number_ok:
            raise InconsistentSettlement(
                f"settlement {settlement_no} is being processed concurrently")
        # Re-check under the lock: a reversal reservation could have been
        # accepted between the check above and this point.
        for name, dsn in (("debit", dsn_debit), ("credit", dsn_credit)):
            if db.reversal_number_exists(dsn, settlement_no):
                raise InconsistentSettlement(
                    f"{settlement_no} is already accepted as a reversal number; "
                    "an ordinary settlement cannot take it")
        _run_settlement(dsn_debit, dsn_credit, settlement_no, amount, gid, failpoint)


def _run_settlement(dsn_debit: str, dsn_credit: str, settlement_no: str,
                    amount: float, gid: str, failpoint: str) -> None:
    # ---- Phase 1: prepare both databases --------------------------------
    db.prepare_business_tx(dsn_debit, settlement_no, "DEBIT", amount, gid)
    db.prepare_business_tx(dsn_credit, settlement_no, "CREDIT", amount, gid)
    # The 'prepared' log rows are committed independently, so they remain
    # visible even if the prepared business tx is later rolled back.
    db.log_event(dsn_debit, settlement_no, db.EVENT_PREPARED, "business tx prepared")
    db.log_event(dsn_credit, settlement_no, db.EVENT_PREPARED, "business tx prepared")

    if failpoint == FAIL_AFTER_BOTH_PREPARED:
        _hard_exit("after_both_prepared")

    # ---- Phase 2: durable decision, written to BOTH coordinator logs ----
    db.log_event(dsn_debit, settlement_no, db.EVENT_DECISION_COMMIT, "decision: commit")
    db.log_event(dsn_credit, settlement_no, db.EVENT_DECISION_COMMIT, "decision: commit")

    # ---- Phase 3: finalize ----------------------------------------------
    debit_outcome = db.commit_prepared(dsn_debit, gid)
    db.log_event(dsn_debit, settlement_no, db.EVENT_COMMITTED,
                 f"COMMIT PREPARED result={debit_outcome}")

    if failpoint == FAIL_AFTER_ONE_COMMIT:
        _hard_exit("after_one_commit")

    credit_outcome = db.commit_prepared(dsn_credit, gid)
    db.log_event(dsn_credit, settlement_no, db.EVENT_COMMITTED,
                 f"COMMIT PREPARED result={credit_outcome}")


def _decide(dsn_debit: str, dsn_credit: str,
            settlement_no: str, debit_prepared: bool,
            credit_prepared: bool) -> tuple[str, str]:
    """Return (decision, basis) from the coordinator logs."""
    debit_log = {row["event"] for row in db.get_logs(dsn_debit, settlement_no)}
    credit_log = {row["event"] for row in db.get_logs(dsn_credit, settlement_no)}
    all_events = debit_log | credit_log

    if db.EVENT_DECISION_COMMIT in all_events:
        where = []
        if db.EVENT_DECISION_COMMIT in debit_log:
            where.append("debit")
        if db.EVENT_DECISION_COMMIT in credit_log:
            where.append("credit")
        return "commit", f"decision_commit found in {','.join(where)} log"

    if db.EVENT_DECISION_ABORT in all_events:
        where = []
        if db.EVENT_DECISION_ABORT in debit_log:
            where.append("debit")
        if db.EVENT_DECISION_ABORT in credit_log:
            where.append("credit")
        return "abort", f"decision_abort found in {','.join(where)} log"

    # No decision recorded anywhere.
    if debit_prepared and credit_prepared:
        return "abort", "no decision recorded; both databases still prepared -> abort"

    # One side is already finalized without any durable decision and the
    # other still holds a prepared transaction: we cannot tell whether the
    # finalized side committed or rolled back. Touching the remaining
    # prepared tx could create an inconsistent split, so preserve it.
    if debit_prepared != credit_prepared:
        return "hold", (
            "no decision recorded and only one database still holds the "
            "prepared transaction; cannot verify the finalized side"
        )

    return "hold", "no decision recorded and no prepared transaction remains; cannot verify"


def recover_settlement(dsn_debit: str, dsn_credit: str,
                       settlement_no: str) -> RecoveryResult:
    """Resume one interrupted settlement. Safe to call concurrently/repeatedly."""
    settlement_no = validate_settlement_no(settlement_no)
    gid = gid_for(settlement_no)

    # A reversal number is not a settlement. Recovery must be entered through
    # the reversal endpoint, which knows the reversal GID and the registry.
    if db.reversal_number_exists(dsn_debit, settlement_no) \
            or db.reversal_number_exists(dsn_credit, settlement_no):
        return RecoveryResult(
            settlement_no, "held",
            "this number was accepted as a reversal; use reversal recovery "
            "(POST /api/reversal/recover) instead")

    # The debit database hosts the advisory lock that serializes recovery.
    with db.recovery_lock(dsn_debit, settlement_no) as acquired:
        if not acquired:
            return RecoveryResult(
                settlement_no, "duplicate",
                "another recovery for this settlement is already running; request rejected",
            )

        debit_prepared = db.prepared_exists(dsn_debit, gid)
        credit_prepared = db.prepared_exists(dsn_credit, gid)

        # Nothing left to recover. Distinguish completed business from an
        # unknown settlement number for the operator UI.
        if not debit_prepared and not credit_prepared:
            debit_done = db.has_ledger_entry(dsn_debit, settlement_no)
            credit_done = db.has_ledger_entry(dsn_credit, settlement_no)
            if debit_done and credit_done:
                return RecoveryResult(
                    settlement_no, "committed",
                    "no residual prepared transaction; ledger entries exist in both databases",
                    {"debit_prepared": False, "credit_prepared": False})
            if not debit_done and not credit_done:
                return RecoveryResult(
                    settlement_no, "noop",
                    "no prepared transaction and no ledger entry for this settlement number",
                    {"debit_prepared": False, "credit_prepared": False})
            return RecoveryResult(
                settlement_no, "held",
                "no prepared transaction remains but ledger entries exist in only one "
                "database; human investigation required",
                {"debit_ledger": debit_done, "credit_ledger": credit_done})

        decision, basis = _decide(dsn_debit, dsn_credit, settlement_no,
                                  debit_prepared, credit_prepared)

        if decision == "hold":
            return RecoveryResult(
                settlement_no, "held", basis,
                {"debit_prepared": debit_prepared, "credit_prepared": credit_prepared})

        if decision == "commit":
            # Durable decision first, so a crash during recovery still leaves
            # an unambiguous trail even if one log was previously missing it.
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_COMMIT not in {
                        r["event"] for r in db.get_logs(dsn, settlement_no)}:
                    db.log_event(dsn, settlement_no, db.EVENT_DECISION_COMMIT,
                                 "decision replicated during recovery")
            outcomes = {}
            if debit_prepared:
                outcomes["debit"] = db.commit_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, settlement_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.commit_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, settlement_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['credit']}")

        else:  # decision == "abort"
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_ABORT not in {
                        r["event"] for r in db.get_logs(dsn, settlement_no)}:
                    db.log_event(dsn, settlement_no, db.EVENT_DECISION_ABORT,
                                 "decision recorded during recovery")
            outcomes = {}
            if debit_prepared:
                outcomes["debit"] = db.rollback_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, settlement_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.rollback_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, settlement_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['credit']}")

        # Terminal state only after BOTH databases have been finalized.
        still_debit = db.prepared_exists(dsn_debit, gid)
        still_credit = db.prepared_exists(dsn_credit, gid)
        if still_debit or still_credit:
            return RecoveryResult(
                settlement_no, "held",
                "finalization incomplete; prepared transaction preserved for retry",
                {"debit_prepared": still_debit, "credit_prepared": still_credit})

        return RecoveryResult(
            settlement_no, "committed" if decision == "commit" else "aborted",
            basis, {"finalize": outcomes,
                    "debit_prepared": False, "credit_prepared": False})
