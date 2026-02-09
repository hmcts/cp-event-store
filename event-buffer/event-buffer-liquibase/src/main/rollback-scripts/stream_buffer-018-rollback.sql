-- Roll back liquibase script '018-create-stream_error_retry-table.changelog.xml'

DROP TABLE stream_error_retry CASCADE;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-018';
