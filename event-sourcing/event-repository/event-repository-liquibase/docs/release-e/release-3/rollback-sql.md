# Release 3 Rollback SQLs (Rollback-C: to Framework D)

## DML

```sql

-- add events whose event_number is null to pre_publish_queue
INSERT INTO pre_publish_queue (SELECT id, date_created FROM event_log WHERE event_number is null and event_status='HEALTHY') ON CONFLICT DO NOTHING;

-- update the sequence 
SELECT setval('event_sequence_seq', (SELECT MAX(event_number) FROM event_log));

-- restore event_number column from sequence
UPDATE event_log el
SET event_number = orderedEl.newEventNumber
    FROM (
         SELECT
             id,
             date_created,
             nextval('event_sequence_seq') as newEventNumber
         FROM event_log WHERE event_number is null AND event_status='HEALTHY'
         ORDER BY date_created ASC 
     ) orderedEl
WHERE el.id = orderedEl.id AND el.event_number is null AND event_status='HEALTHY';

-- add published events 
INSERT INTO public.published_event (
    id,
    stream_id,
    position_in_stream,
    "name",
    payload,
    metadata,
    date_created,
    event_number,
    previous_event_number
)
SELECT
    el.id,
    el.stream_id,
    el.position_in_stream,
    el."name",
    el.payload,
    el.metadata,
    el.date_created,
    el.event_number,
    el.previous_event_number
FROM public.event_log AS el
WHERE el.is_published = TRUE
  AND el.event_number > (select MAX(event_number) from published_event)
  AND el.event_number IS NOT NULL
  AND el.previous_event_number IS NOT NULL
ORDER BY el.event_number
    -- to support idempotency
    ON CONFLICT (id) DO NOTHING;  
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

