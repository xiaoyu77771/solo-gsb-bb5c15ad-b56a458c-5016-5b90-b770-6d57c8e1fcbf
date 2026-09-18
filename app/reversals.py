"""Event reversals: append a compensating event that undoes an applied event.

A reversal request carries a client-assigned ``reversal_id`` (a *global*
idempotency key), the target ``stream_id`` and ``event_id`` (taken from the
URL), and reverses exactly one already-*applied* event that has not itself been
reversed yet. On success an ordinary event is appended at the stream's current
``next_sequence`` with id ``reversal:<reversal_id>`` and ``delta`` equal to the
*negation* of the original event's delta, then run through the ordinary
contiguous catch-up (the append may also unblock a buffered run of previously
gap-parked events). The stream's ``balance`` / ``next_sequence`` row is updated
and a ``reversals`` record is written — all inside one ``BEGIN IMMEDIATE``
transaction. Any failure rolls the whole operation back.

Because the compensating entry is a plain ``events`` row it is, like transfer
events, observable through every existing interface: ``GET /streams/{id}``
balances, consumer claim/ack batches and forks.

A retried request with the same ``reversal_id`` and the same target returns the
stored original result (HTTP 200) together with the *latest* stream state and
never compensates twice; the same ``reversal_id`` aimed at a different target
is a ``409``. Reversing the same original event under a *different*
``reversal_id`` is also a ``409`` — the ``UNIQUE (stream_id, original_event_id)``
constraint is the concurrency-safe guarantee, so parallel requests can never
produce a double compensation.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import db, repo


class ReversalError(Exception):
    """Raised when a reversal request is invalid or clashes (HTTP 409)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class ReversalResult:
    reversal_id: str
    stream_id: str
    original_event_id: str
    original_sequence: int
    original_delta: int
    reversal_event_id: str
    reversal_sequence: int
    state: repo.StreamState
    replayed: bool


def reversal_event_id(reversal_id: str) -> str:
    return f"reversal:{reversal_id}"


def create_reversal(
    stream_id: str, event_id: str, reversal_id: str
) -> ReversalResult | None:
    """Reverse one applied, not-yet-reversed event of a stream.

    Returns the created/existing :class:`ReversalResult`, or ``None`` when the
    stream does not exist (HTTP 404). Raises :class:`ReversalError` when the
    target event is missing/applied-state-invalid (HTTP 409) or the idempotent
    replay contradicts what is stored (HTTP 409).
    """
    with db.write_transaction() as conn:
        existing = conn.execute(
            "SELECT reversal_id, stream_id, original_event_id, original_sequence, "
            "original_delta, reversal_sequence FROM reversals WHERE reversal_id = ?",
            (reversal_id,),
        ).fetchone()
        if existing is not None:
            # Global idempotency: reversal_id alone identifies the request, and
            # a replay must target the very same stream/event.
            if (
                existing["stream_id"] != stream_id
                or existing["original_event_id"] != event_id
            ):
                raise ReversalError(
                    "reversal_replay_conflict",
                    f"reversal_id {reversal_id!r} already exists for a different "
                    "target stream/event",
                )
            # Return the original coordinates but the *current* stream state
            # (later events may have been applied since the reversal).
            row = conn.execute(
                "SELECT balance, next_sequence FROM streams WHERE stream_id = ?",
                (stream_id,),
            ).fetchone()
            return ReversalResult(
                reversal_id=reversal_id,
                stream_id=stream_id,
                original_event_id=event_id,
                original_sequence=existing["original_sequence"],
                original_delta=existing["original_delta"],
                reversal_event_id=reversal_event_id(reversal_id),
                reversal_sequence=existing["reversal_sequence"],
                state=repo.build_state(
                    conn, stream_id, row["balance"], row["next_sequence"]
                ),
                replayed=True,
            )

        stream = conn.execute(
            "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream is None:
            return None

        original = conn.execute(
            "SELECT event_id, sequence, delta, applied FROM events "
            "WHERE stream_id = ? AND event_id = ?",
            (stream_id, event_id),
        ).fetchone()
        if original is None:
            raise ReversalError(
                "event_not_found",
                f"event {event_id!r} does not exist in stream {stream_id!r}",
                status_code=404,
            )
        if original["applied"] == 0:
            raise ReversalError(
                "event_not_applied",
                f"event {event_id!r} in stream {stream_id!r} has not been applied "
                "yet (buffered behind a gap); only applied events can be reversed",
            )

        already = conn.execute(
            "SELECT reversal_id FROM reversals "
            "WHERE stream_id = ? AND original_event_id = ?",
            (stream_id, event_id),
        ).fetchone()
        if already is not None:
            # Same original event, but a brand-new reversal_id: never
            # compensate the same event twice.
            raise ReversalError(
                "event_already_reversed",
                f"event {event_id!r} in stream {stream_id!r} was already reversed "
                f"by reversal_id {already['reversal_id']!r}",
            )

        sequence = stream["next_sequence"]
        compensating_delta = -original["delta"]
        new_event_id = reversal_event_id(reversal_id)

        # The sequence slot is the stream's next_sequence, which is always free
        # (it is the exclusive end of the applied prefix); UNIQUE constraints on
        # events plus the reversals keys are defence in depth under concurrency.
        conn.execute(
            "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
            "VALUES (?, ?, ?, ?, 0)",
            (stream_id, new_event_id, sequence, compensating_delta),
        )

        # Ordinary contiguous catch-up, inside the same transaction: the
        # compensating event (and any gap-parked run it unblocks) is folded in.
        balance, next_sequence = repo.apply_contiguous(
            conn, stream_id, stream["balance"], sequence
        )

        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (balance, next_sequence, stream_id),
        )
        conn.execute(
            "INSERT INTO reversals (reversal_id, stream_id, original_event_id, "
            "original_sequence, original_delta, reversal_sequence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                reversal_id,
                stream_id,
                event_id,
                original["sequence"],
                original["delta"],
                sequence,
            ),
        )

        return ReversalResult(
            reversal_id=reversal_id,
            stream_id=stream_id,
            original_event_id=event_id,
            original_sequence=original["sequence"],
            original_delta=original["delta"],
            reversal_event_id=new_event_id,
            reversal_sequence=sequence,
            state=repo.build_state(conn, stream_id, balance, next_sequence),
            replayed=False,
        )
