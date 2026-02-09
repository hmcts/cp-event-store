-- Roll back liquibase script '018-add-retry-columns-to-stream_error-table.changelog.xml'

ALTER TABLE stream_error DROP COLUMN updated_at;
ALTER TABLE stream_error DROP COLUMN retry_count;
ALTER TABLE next_retry_time DROP COLUMN updated_at;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-018';
