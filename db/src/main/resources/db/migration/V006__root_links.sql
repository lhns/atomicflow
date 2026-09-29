-- The top-level instance of the tree an instance belongs to (itself for top-level instances).
-- Child instances run inline in their root's pass, so the root is what gets woken up and cancelled.
ALTER TABLE workflow_instance
    ADD root_workflow_id UUID,
    ADD root_instance_id UUID;
