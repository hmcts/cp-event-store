# Framework E Rollout and Rollback SQL Commands

This document provides the required SQL commands for migrating from Framework D to Framework E.
> **Note**: Some sections link to SQL to avoid duplication. These links will be replaced with inline SQL after full testing and before sharing with DBAs.

## D to E compatibility mode

This involves both DDL (schema changes) and DML (data updates) operations. Due to the potentially long execution time of DML commands on large datasets, the rollout is designed to be executed in multiple phases:

- **Phase 1**: Execute DDL and DML changes while the application remains on Framework D (during off-peak hours)
- **Phase 2**: Deploy Framework E code changes while turning on compatibility mode

---

## Phase 1

### Rollout SQLs

#### DDL

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

--Update databasechangelog 
TBD

```

#### Phase-1 DML

```sql
-- update is_published flag on event range partition
UPDATE event_log el
SET is_published = true
WHERE
  el.is_published IS DISTINCT FROM true
  AND el.event_number IS NOT NULL
  AND el.event_number BETWEEN :range_start AND :range_end
  AND EXISTS (
        SELECT 1
        FROM published_event pe
        WHERE pe.event_number = el.event_number
  );

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

### Phase-1 Rollback SQLs

#### DDL

```sql
DROP INDEX IF EXISTS event_log_date_created_without_event_number_idx;

ALTER TABLE event_log
    ALTER COLUMN event_number SET NOT NULL;

ALTER TABLE event_log
    DROP COLUMN IF EXISTS is_published,
    DROP COLUMN IF EXISTS previous_event_number,
    DROP COLUMN IF EXISTS event_status; 

--Update databasechangelog 
TBD

```
---

## Phase 2

### Rollout SQLs

> **Note**: If phase-1 and phase-2 are planned in the same deployment window (valid for small to medium contexts), rollout SQLs should be skipped.

#### DML
Execute [Phase-1 DML](#phase-1-dml), then execute below
```sql
-- Deal with inprogress events in pre_publish_queue table
TBD
```

### Phase-2 Rollback SQLs

#### DML
```sql
-- Deal with inprogress events (having event_number=null) and add it to pre_publish_queue table
TBD
```

#### DDL

Execute [Phase 1 Rollback SQLs](#phase-1-rollback-sqls)

---

## E compatibility to full feature mode

### Rollout SQLs

N/A

### Rollback SQLs

#### DML
```sql
-- Copy data from event_log to published_event table of those events that arrived after turning on full feature mode
TBD
```

#### DDL

Execute [Phase 2 Rollback SQLs](#phase-2-rollback-sqls)

