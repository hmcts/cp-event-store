# Release 1 Rollback SQLs (From compatability feature mode to Framework D)

## DML

```sql
-- PROCESS IN-FLIGHT EVENTS
-- process whose event_number is null
INSERT INTO pre_publish_queue (SELECT id, date_created FROM event_log WHERE event_number is null and event_status='HEALTHY') ON CONFLICT DO NOTHING;

WITH ordered AS (
    SELECT id
    FROM event_log
    WHERE event_number IS NULL
    AND event_status = 'HEALTHY'
    ORDER BY date_created ASC
)
UPDATE event_log el
SET event_number = nextval('event_sequence_seq')
FROM ordered o
WHERE el.id = o.id;
```

## DDL

```sql
DROP INDEX IF EXISTS event_log_date_created_without_event_number_idx;

ALTER TABLE event_log
    ALTER COLUMN event_number SET NOT NULL;

ALTER TABLE event_log
    DROP COLUMN IF EXISTS is_published,
    DROP COLUMN IF EXISTS previous_event_number,
    DROP COLUMN IF EXISTS event_status; 

-- Delete databasechangelog entry
DELETE FROM public.databasechangelog
WHERE id = 'event-store-026'
  AND author = 'TechPod'
  AND filename = '026-global-sequencing-on-event-log-table.changelog.xml';
```

