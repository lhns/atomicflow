-- When cancellation was requested for an instance. It stays requested until the workflow finishes.
ALTER TABLE workflow_instance
    ADD cancel_requested_at TIMESTAMPTZ;
