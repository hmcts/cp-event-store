# Release 3 Rollback SQLs (Rollback-C: to Framework D)

## DML

N/A

## DDL

```sql
DROP TABLE IF EXISTS pre_publish_queue;
DROP TABLE IF EXISTS published_event;
DROP SEQUENCE IF EXISTS event_sequence_seq;
```

