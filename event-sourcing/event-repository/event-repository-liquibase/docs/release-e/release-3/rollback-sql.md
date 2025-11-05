# Release 3 Rollback SQLs (Rollback-C: to Framework D)

## DML

```sql

-- add events whose event_number is null to pre_publish_queue
INSERT INTO pre_publish_queue (SELECT id, date_created FROM event_log WHERE event_number is null and event_status='HEALTHY') ON CONFLICT DO NOTHING;

-- restore event_number column from sequence
update event_log el
set event_number = orderedEl.newEventNumber
    from (
         SELECT
             id,
             date_created,
             nextval('event_sequence_seq') as newEventNumber
         from event_log where event_number is null and event_status='HEALTHY'
         order by date_created ASC 
     ) orderedEl
where el.id = orderedEl.id and el.event_number is null and event_status='HEALTHY';

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
  --AND el.event_number > :event_number
  AND el.event_number IS NOT NULL
  AND el.previous_event_number IS NOT NULL
ORDER BY el.event_number
    -- to support idempotency
    ON CONFLICT (id) DO NOTHING;  
```

## DDL

Execute [Release 2 Rollback SQLs](../release-2/rollback-sql.md)

