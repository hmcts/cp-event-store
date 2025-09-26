-- Roll back liquibase script '029-change-event_log-event_number-definition.changelog.xml'

ALTER TABLE event_log ALTER COLUMN event_number DROP NOT NULL;
ALTER TABLE event_log ALTER COLUMN event_number DROP DEFAULT;
