# Release 2 Rollout SQLs

> **Note**: If release-1 and release-2 are planned in the same deployment window (valid for small to medium contexts), this can be skipped

## DML

Execute [Release 1 DML](../release-1/rollout-sql.md#dml), then execute below

```sql

-- Delta update for is_published=true 
DO $$
DECLARE deltaStartFrom bigint;
BEGIN
select COALESCE(max(event_number), 0) into deltaStartFrom from event_log where is_published=true;

UPDATE event_log el
SET is_published = true
WHERE
    el.is_published IS DISTINCT FROM true
    AND el.event_number IS NOT NULL
    AND el.event_number > deltaStartFrom
    AND EXISTS (
    SELECT 1
    FROM published_event pe
    WHERE pe.event_number = el.event_number);

END $$;


-- process in-flight events in pre_publish_queue
UPDATE event_log el SET event_number=null, event_status='HEALTHY' 
WHERE EXISTS (SELECT event_log_id FROM pre_publish_queue ppq 
            WHERE ppq.event_log_id=el.id);

-- clean pre_publish_queue
DELETE FROM pre_publish_queue

```

