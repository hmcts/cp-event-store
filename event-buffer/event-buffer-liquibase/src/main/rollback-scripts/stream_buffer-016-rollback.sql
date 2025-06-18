-- Roll back liquibase script '015-add-stream-statistic-table.changelog.xml'
DROP TABLE IF EXISTS stream_statistic;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-016';
