-- A step_cache row with a NULL output marks an at-most-once step whose body started but recorded no outcome yet.
ALTER TABLE step_cache
    ALTER COLUMN output DROP NOT NULL;
