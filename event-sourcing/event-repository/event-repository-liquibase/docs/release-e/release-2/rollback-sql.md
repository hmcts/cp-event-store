# Release 2 Rollback SQLs (Rollback-B: to Framework D)

## DML

```sql

-- add events whose event_number is null to pre_publish_queue
INSERT INTO pre_publish_queue (SELECT id, date_created FROM event_log WHERE event_number is null and event_status='HEALTHY') ON CONFLICT DO NOTHING;

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
```

## DDL

Execute [Release 1 Rollback SQLs](../release-1/rollback-sql.md)

