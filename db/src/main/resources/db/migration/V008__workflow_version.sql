-- The version of the workflow's code that created an instance (instances created before are version 1).
ALTER TABLE workflow_instance
    ADD workflow_version INT NOT NULL DEFAULT 1;
