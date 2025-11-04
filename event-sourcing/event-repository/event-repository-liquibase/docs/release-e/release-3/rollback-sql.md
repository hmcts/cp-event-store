# Release 3 Rollback SQLs (Rollback-C: to Framework D)

## DML

```sql
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

