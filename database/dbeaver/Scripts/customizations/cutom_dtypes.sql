DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'app_type'
    ) THEN
        CREATE TYPE app_type AS ENUM ('game', 'dlc', 'demo', 'series', 'episode', 'music', 'mod');
    END IF;
END
$$;





-- Search for Custom TYPES
SELECT n.nspname AS schema,
       t.typname AS type_name,
       t.typtype,
       t.typcategory
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND t.typtype IN ('e', 'c', 'd'); -- e = enum, c = composite, d = DOMAIN