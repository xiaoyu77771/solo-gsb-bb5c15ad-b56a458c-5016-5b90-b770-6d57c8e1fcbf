"""Stdlib-only smoke test for event reversals (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "reversals.db")

from app import db, repo, consumers, forks, reversals  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# Stream "a": applied events 0..3 (100, -30, 20, 10) -> balance 100, next 4.
for seq, delta in [(0, 100), (1, -30), (2, 20), (3, 10)]:
    repo.submit_event("a", f"e{seq}", seq, delta)
# A buffered event parked behind a gap at 5 (sequence 6).
repo.submit_event("a", "e6", 6, 7)
base = repo.get_stream_state("a")
check("baseline", base.balance == 100 and base.next_sequence == 4
      and base.pending_sequences == [6], str(base))

# 1. happy path: compensating event with the negated delta at next_sequence
r = reversals.create_reversal("a", "e1", "rev-1")
check("reversal created", r is not None and not r.replayed, str(r))
check(
    "reversal coordinates",
    r.original_sequence == 1 and r.original_delta == -30
    and r.reversal_event_id == "reversal:rev-1"
    and r.reversal_sequence == 4,
    str(r),
)
st = repo.get_stream_state("a")
check("balance compensated", st.balance == 130 and st.next_sequence == 5
      and st.pending_sequences == [6], str(st))

# 2. idempotent replay: same reversal_id + same target -> 200 semantics
r2 = reversals.create_reversal("a", "e1", "rev-1")
check("replay flagged", r2.replayed)
check("replay same coordinates", r2.reversal_sequence == 4
      and r2.original_sequence == 1, str(r2))
check("no double compensation", repo.get_stream_state("a").balance == 130)

# 3. replay reports the *current* state after later events
repo.submit_event("a", "a-extra", 5, 4)  # closes gap 5 -> applies 5 and parked 6
r3 = reversals.create_reversal("a", "e1", "rev-1")
check("replay sees current state", r3.state.balance == 141
      and r3.state.next_sequence == 7 and r3.state.pending_sequences == []
      and r3.reversal_sequence == 4, str(r3.state))

# 4. same original event, different reversal_id -> 409
try:
    reversals.create_reversal("a", "e1", "rev-1-again")
    check("second reversal of same event rejected", False)
except reversals.ReversalError as e:
    check("second reversal -> event_already_reversed",
          e.code == "event_already_reversed" and e.status_code == 409, e.code)

# 5. reversal_id reused for a different target -> 409
repo.submit_event("b", "b0", 0, 50)
for stream_id, event_id in (("a", "e0"), ("b", "b0")):
    try:
        reversals.create_reversal(stream_id, event_id, "rev-1")
        check(f"global drift rejected {stream_id}", False)
    except reversals.ReversalError as e:
        check(f"global drift -> reversal_replay_conflict ({stream_id})",
              e.code == "reversal_replay_conflict", e.code)

# 6. unknown stream -> None (HTTP 404); unknown event -> event_not_found 404
check("unknown stream -> None",
      reversals.create_reversal("ghost", "x", "rev-404s") is None)
try:
    reversals.create_reversal("a", "missing-event", "rev-404e")
    check("unknown event rejected", False)
except reversals.ReversalError as e:
    check("unknown event -> event_not_found 404",
          e.code == "event_not_found" and e.status_code == 404, e.code)

# 7. unapplied (buffered behind a gap) event -> 409 event_not_applied
repo.submit_event("c", "c1", 1, 9)  # buffered: gap at 0
try:
    reversals.create_reversal("c", "c1", "rev-pending")
    check("unapplied event rejected", False)
except reversals.ReversalError as e:
    check("unapplied event -> event_not_applied 409",
          e.code == "event_not_applied" and e.status_code == 409, e.code)
# once the gap closes the event becomes reversible
repo.submit_event("c", "c0", 0, 100)
rc = reversals.create_reversal("c", "c1", "rev-pending")
check("applied event reversible after catch-up", rc is not None and not rc.replayed
      and rc.state.balance == 100 and rc.reversal_sequence == 2, str(rc))

# 8. compensating event is consumable through claim/ack
claim = consumers.claim("a", "worker", 100)
check("reversal event consumable",
      [(ev.sequence, ev.event_id, ev.delta) for ev in claim.events][4]
      == (4, "reversal:rev-1", 30), str(claim.events))
check("ack reversal batch", consumers.ack("a", "worker", claim.lease_token) == 7)

# 9. reversals are visible to forks: fork through the reversal sequence
f = forks.create_fork("a", "fork-after-rev", "a-fork", 5)
check("fork prefix includes compensating event",
      f.balance == 130 and f.next_sequence == 5, str(f))
claim_f = consumers.claim("a-fork", "w", 100)
check("forked events include reversal",
      [(ev.sequence, ev.delta) for ev in claim_f.events]
      == [(0, 100), (1, -30), (2, 20), (3, 10), (4, 30)],
      str(claim_f.events))

# 10. a reversal can itself be reversed (its compensating event is a normal event)
rr = reversals.create_reversal("a", "reversal:rev-1", "rev-of-rev")
check("reversal of a reversal", rr is not None and not rr.replayed
      and rr.original_delta == 30 and rr.reversal_sequence == 7
      and rr.state.balance == 111, str(rr))

# 11. concurrent identical retries commit exactly one compensation
repo.submit_event("d", "d0", 0, 100)
results = []
errors = []


def do_retry():
    try:
        results.append(reversals.create_reversal("d", "d0", "rev-race"))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_retry) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent retries no errors", not errors, str(errors[:1]))
check("concurrent retries one commit", len(results) == 20
      and sum(1 for x in results if not x.replayed) == 1
      and sum(1 for x in results if x.replayed) == 19)
sd = repo.get_stream_state("d")
check("concurrent balance compensated once",
      sd.balance == 0 and sd.next_sequence == 2, str(sd))

# 12. concurrent reversals of the same event with distinct ids never double-pay
repo.submit_event("k", "k0", 0, 100)
k_errors = []


def do_distinct(n):
    try:
        reversals.create_reversal("k", "k0", f"rev-k-{n}")
    except reversals.ReversalError as e:
        k_errors.append(e.code)
    except Exception:
        k_errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_distinct, args=(n,)) for n in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("exactly one distinct-id reversal wins",
      len(k_errors) == 19 and set(k_errors) == {"event_already_reversed"},
      str(k_errors[:3]))
sk = repo.get_stream_state("k")
check("distinct-id race compensated once",
      sk.balance == 0 and sk.next_sequence == 2, str(sk))
with db.read_connection() as conn:
    n = conn.execute("SELECT COUNT(*) FROM reversals WHERE stream_id = 'k'").fetchone()[0]
check("one reversals row", n == 1, str(n))

# 13. persistence: reopen and the reversals row is still there
with db.read_connection() as conn:
    row = conn.execute(
        "SELECT stream_id, original_event_id, original_delta, reversal_sequence "
        "FROM reversals WHERE reversal_id = 'rev-1'"
    ).fetchone()
check("reversal persisted", tuple(row) == ("a", "e1", -30, 4),
      str(tuple(row) if row else None))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL REVERSAL CHECKS PASSED")
