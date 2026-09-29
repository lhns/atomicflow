-- The owner of the current execution lock: releasing, renewing and writing step state require owning it.
ALTER TABLE workflow_instance
    ADD lock_owner UUID;
