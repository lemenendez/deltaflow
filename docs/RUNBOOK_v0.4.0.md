# DeltaFlow v0.4.0 Lease Runbook

This runbook covers the common lease operations for the v0.4 hardening work: worker crash, heartbeat failure, and ownership conflicts.

## Signals to Watch

Look for these structured log events:

- `worker_heartbeat_renew_failed`
- `worker_heartbeat_stopped`
- `lease_renew_failed`
- `lease_renew_rejected`
- `lease_transition_rejected`
- `lease_claimed` with `reason=expired_reclaimed`

The worker claim and heartbeat events are intentionally lighter-weight than the store transition events, so focus on their event name plus the lease ownership fields they do carry.

The expected failure mode is that the worker cancels in-flight work as soon as lease renewal fails, and retry/dead handling prefers the lease error over downstream `context.Canceled` noise.

## Worker Crash

1. Confirm the worker process is gone or no longer making heartbeat progress.
2. Check the job state and `locked_until` timestamp.
3. If the lease has not expired yet, wait for it to expire before taking action.
4. Expect another worker to reclaim the job with `lease_claimed` and `reason=expired_reclaimed`.
5. Verify there is only one active `processing` owner for the job.

## Heartbeat Failure

1. Treat `worker_heartbeat_renew_failed` as a loss of ownership.
2. Stop trusting the current worker result once renewal has failed.
3. Expect the worker to cancel its per-job context and stop the projector/applier path.
4. Let the job transition to retrying or dead based on the next failure recorded after lease loss.
5. Do not manually mark the job complete unless ownership is still valid.

## Ownership Conflict

1. Treat `lease_transition_rejected` or `lease_renew_rejected` as a sign that the worker does not currently own the lease.
2. Check whether another worker already reclaimed the job or whether `locked_until` expired.
3. Do not force a state transition while the active lease belongs to someone else.
4. If the lease is stale and the job is safe to replay, allow a clean reclaim instead of overriding ownership.
5. If a force action is needed, require an explicit audit reason and confirm the job is not actively processing elsewhere.

## Safe Operator Actions

- Prefer waiting for lease expiry over manual intervention.
- Prefer reclaiming an expired job over forcing a transition on an owned job.
- Use the most specific log event and job timestamps before deciding whether the issue is crash, heartbeat loss, or ownership rejection.

## Quick Verification

After recovery, confirm:

- the job has one current owner or has moved to a terminal state
- no worker keeps renewing an expired or rejected lease
- the next `lease_claimed` event is either a normal claim or an explicit `expired_reclaimed` reclaim
