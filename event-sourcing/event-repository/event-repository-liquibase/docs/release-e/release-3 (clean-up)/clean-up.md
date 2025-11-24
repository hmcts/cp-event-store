# Cleanup SQLs

## DML

N/A

## DDL

```sql
DROP TABLE IF EXISTS pre_publish_queue;
DROP TABLE IF EXISTS published_event;

ALTER TABLE event_log
    ALTER COLUMN event_number DROP DEFAULT;

DROP SEQUENCE IF EXISTS event_sequence_seq;
```

