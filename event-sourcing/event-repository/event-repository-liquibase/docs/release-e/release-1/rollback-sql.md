# Release 1 Rollback SQLs (Rollback-A: to Framework D)

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

