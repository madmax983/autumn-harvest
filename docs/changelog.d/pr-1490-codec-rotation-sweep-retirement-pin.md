## Phase — Close the sweep-batch-vs-retirement race in codec rotation (issue #1251)

Fixes a narrow but serious race in the codec-rotation re-encryption sweep
(issue #948): a re-encryption batch could commit a row under a key that
`retire_codec_key` had already retired, making that row undecodable —
permanently, if the operator destroyed the key material on a green gate.

**The race.** `sweep_codec_reencryption_once` resolves its target key once
for the whole batch and writes every row under that exact id. Between
resolving the target and a later row's compare-and-swap, a second rotation
could move the active key away from the batch's target, and
`retire_codec_key`'s census — reading zero rows, because the batch had not
committed anything yet — would let the target be retired out from under the
still-running batch.

**The fix.** `PayloadCodecs` gains a process-local pin count per key id
(`pin_key_for_sweep`, an RAII guard). The sweep pins its target for the
whole batch, before writing a single row. `retire_key_local` refuses a
pinned key under the same write lock that removes it, so there is no window
between the check and the removal — retirement of a pinned key fails
closed, by construction, not by timing. `validate_retirement_request` also
checks the pin up front, so a retirement already known to be blocked skips
the per-shard census entirely.

This is a third, process-local gap alongside the two fleet-wide gaps
`FleetWriteFence` already documents (another worker still writing under the
key; an in-flight append not yet committed). Unlike those, it needs no
operator attestation — it is closed automatically by the pin.

**Tests.** `payload_codec.rs` unit tests cover the pin/unpin mechanics
directly (refusal while pinned, release on drop, reference counting across
two overlapping pins). A new DB integration test in
`codec_rotation_db_tests.rs`,
`a_batch_pinned_to_a_key_blocks_its_retirement_through_a_double_rotation`,
drives the exact interleaving from the issue: pin the target, rotate twice,
attempt retirement (refused), let the batch commit its row under the
pinned target, and assert the row lands under it and history stays fully
decodable — then confirms retirement still correctly refuses once real rows
exist, and succeeds once the row migrates on and the pin releases.

No migration, no new `WorkflowEvent` variant, no change to the append-only
event log itself — the fix is entirely in the in-process codec registry and
the retirement gate that consults it.

`docs/operations/codec-key-rotation.md` documents the third gap and that it
closes automatically.
