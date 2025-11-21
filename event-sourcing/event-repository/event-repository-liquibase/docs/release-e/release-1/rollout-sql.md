# Release 1 Rollout SQLs (DB Only release)

## DDL

```sql
ALTER TABLE event_log
    ADD COLUMN previous_event_number BIGINT;

ALTER TABLE event_log
    ADD COLUMN is_published BOOLEAN DEFAULT FALSE;

ALTER TABLE event_log
    ADD COLUMN event_status VARCHAR(15) DEFAULT 'HEALTHY';

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
-- update event_status flag
UPDATE event_log AS el
SET event_status = 'FAULTY'
    FROM event_stream AS es
WHERE es.stream_id = el.stream_id
  AND es.active IS TRUE
  AND el.event_status IS DISTINCT FROM 'FAULTY'
    AND el.event_number IS NOT NULL
    --AND el.event_number BETWEEN :range_start AND :range_end
    AND NOT EXISTS (
    SELECT 1
    FROM published_event AS pe
    WHERE pe.event_number = el.event_number
    );
```

