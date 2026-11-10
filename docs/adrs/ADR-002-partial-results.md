# ADR-002: Return a marked partial transcript after a permanent segment failure

## Title

Return a marked partial transcript after a permanent segment failure.

## Date

2026-11-02

## Status

Accepted

## Context

A 30-minute audio file creates up to 60 segments. A segment may still fail after three total attempts even when the other segments have completed successfully. The system should preserve useful results, but users must be able to see that part of the transcript is missing. The project also needs a bounded retry process so a failed segment does not keep a job in progress forever.

## Decision

Job Service stores one final outcome for each segment using `(job_id, segment_number)`. It waits until every segment is either `DONE` or `FAILED`, orders the outcomes by segment number, removes overlap words, and inserts a timestamped `[TRANSCRIPTION FAILED start-end]` marker for a permanently failed segment. If at least one segment succeeds, the final status is `PARTLY_DONE`; if all segments fail, the status is `FAILED`.

## Consequences

- Successful segments remain available instead of being discarded after one local failure.
- The timestamped marker makes the missing content visible to the user.
- The result is delayed until the slowest segment reaches a final state.
- The transcript is incomplete, so it must not be presented as a fully successful result.
- With a maximum of 60 segments and three total attempts, retry and database growth remain bounded.

## Alternatives considered

1. **Fail the whole job when one segment fails.** This is simpler and guarantees that a `DONE` result is complete, but it discards the successful content from the remaining segments. We rejected it because a local segment failure should not invalidate an otherwise useful 30-minute transcript.

2. **Retry the failed segment indefinitely.** This could eventually recover from a temporary problem, but it would keep the job active without a predictable completion time and could occupy queue capacity. We rejected it in favour of three total attempts.

3. **Append segments immediately as they finish.** This gives earlier visible output, but segments can finish out of order and the user would see unstable or incorrectly ordered text. We rejected it because Job Service must return an ordered transcript.

This decision should be reconsidered if user testing shows that one missing segment makes more than 10% of completed jobs unusable, or if the system is used for legal, medical or compliance purposes where incomplete transcription is unacceptable.
