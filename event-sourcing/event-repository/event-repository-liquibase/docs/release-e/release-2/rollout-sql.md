# Release 2 Rollout SQLs

## DML

Execute [Release 1 DML](../release-1/rollout-sql.md#dml), then execute below

```sql

-- Delta update for event_status (if DB changes and code deployment are done at same time, this delta sql can be skipped, no harm in running it as well)
DO $$
DECLARE deltaStartFrom bigint;
BEGIN
select COALESCE(max(event_number), 0) into deltaStartFrom from event_log where is_published=true;

UPDATE event_log AS el
SET event_status = 'FAULTY'
    FROM event_stream AS es
WHERE es.stream_id = el.stream_id
  AND es.active IS TRUE
  AND el.event_status IS DISTINCT FROM 'FAULTY'
    AND el.event_number IS NOT NULL
    AND el.event_number > deltaStartFrom
    AND NOT EXISTS (
    SELECT 1
    FROM published_event AS pe
    WHERE pe.event_number = el.event_number
    );
END $$;


-- process in-flight events in pre_publish_queue
UPDATE event_log el SET event_number=null, event_status='HEALTHY', is_published=false 
WHERE EXISTS (SELECT event_log_id FROM pre_publish_queue ppq 
            WHERE ppq.event_log_id=el.id);

-- clean pre_publish_queue
DELETE FROM pre_publish_queue

```

