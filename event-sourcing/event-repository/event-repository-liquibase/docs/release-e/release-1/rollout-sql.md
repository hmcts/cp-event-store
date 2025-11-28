# Release 1 Rollout SQLs (From Framework D to compatability feature mode)

## DDL

```sql
ALTER TABLE event_log
    ADD COLUMN previous_event_number BIGINT;

ALTER TABLE event_log
    ADD COLUMN is_published BOOLEAN DEFAULT TRUE;

ALTER TABLE event_log
    ADD COLUMN event_status VARCHAR(64) DEFAULT 'HEALTHY';

CREATE INDEX event_log_date_created_without_event_number_idx
    ON event_log (date_created)
    WHERE event_number IS NULL; 

ALTER TABLE event_log
    ALTER COLUMN event_number DROP NOT NULL;

-- Add entry to databasechangelog 
INSERT INTO public.databasechangelog (
    id,
    author,
    filename,
    dateexecuted,
    orderexecuted,
    exectype,
    md5sum,
    description,
    "comments",
    tag,
    liquibase,
    contexts,
    labels,
    deployment_id
)
VALUES (
           'event-store-026',
           'TechPod',
           '026-global-sequencing-on-event-log-table.changelog.xml',
           NOW(),
           26,
           'EXECUTED',
           '8:e012cb2e1b2f34233815347997fc011b',
           'addColumn tableName=event_log; addColumn tableName=event_log; addColumn tableName=event_log; sql; dropNotNullConstraint columnName=event_number, tableName=event_log; sql',
           '',
           NULL,
           '4.10.0',
           NULL,
           NULL,
           '2160894619'
       );
```

## DML

```sql
-- UPDATE EVENT_STATUS (for contexts with huge data set)
CREATE TABLE IF NOT EXISTS temporary_publish_failed_event_ids (
    event_id UUID PRIMARY KEY
);

-- step-1
INSERT INTO temporary_publish_failed_event_ids (event_id)
SELECT el.id
FROM event_log AS el
         JOIN event_stream AS es
              ON es.stream_id = el.stream_id
WHERE es.active IS TRUE
    AND el.event_number IS NOT NULL
    -- AND el.event_number BETWEEN :range_start AND :range_end
    AND NOT EXISTS (
    SELECT 1
    FROM published_event AS pe
    WHERE pe.event_number = el.event_number
    );

-- step-2
UPDATE event_log AS el
SET event_status = 'PUBLISH_FAILED'
WHERE el.id IN (
    SELECT event_id
    FROM temporary_publish_failed_event_ids
);

-- PROCESS IN-FLIGHT EVENTS
UPDATE event_log el SET event_number=null, event_status='HEALTHY', is_published=false
WHERE EXISTS (SELECT event_log_id FROM pre_publish_queue ppq
              WHERE ppq.event_log_id=el.id);

DELETE FROM pre_publish_queue
```

