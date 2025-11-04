# Release 2 Rollback SQLs (Rollback-B: to Framework D)

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
         from event_log where event_number is null
         order by date_created DESC
     ) orderedEl
where el.id = orderedEl.id and el.event_number is null;
```

## DDL

Execute [Release 1 Rollback SQLs](../release-1/rollback-sql.md)

