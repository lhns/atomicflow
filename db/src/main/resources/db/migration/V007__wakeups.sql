-- Root instances that are due to run: on creation, when a signal is set anywhere in their tree, and for retries.
-- While a run is in progress, its claim_token is set and scheduled_at is postponed by the lock timeout, so a run
-- interrupted by a dead process is retried automatically.
CREATE TABLE workflow_wakeup
(
    root_instance_id UUID PRIMARY KEY REFERENCES workflow_instance (id) ON DELETE CASCADE,
    root_workflow_id UUID        NOT NULL,
    scheduled_at     TIMESTAMPTZ NOT NULL,
    attempts         INT         NOT NULL DEFAULT 0,
    claim_token      UUID
);

CREATE INDEX workflow_wakeup_due ON workflow_wakeup (scheduled_at);
