DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname = 'workflow_status' AND e.enumlabel = 'PAUSED'
    ) THEN
        ALTER TYPE workflow_status ADD VALUE 'PAUSED';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname = 'workflow_status' AND e.enumlabel = 'CANCELLED'
    ) THEN
        ALTER TYPE workflow_status ADD VALUE 'CANCELLED';
    END IF;
END $$;
