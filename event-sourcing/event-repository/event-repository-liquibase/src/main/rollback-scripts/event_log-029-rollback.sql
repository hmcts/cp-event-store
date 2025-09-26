-- Roll back liquibase script '029-change-event_log-event_number-definition.changelog.xml'

ALTER TABLE event_log ALTER COLUMN event_number SET DEFAULT VALUE nextval('event_sequence_seq'::regclass);;
ALTER TABLE event_log ALTER COLUMN event_number SET NOT NULL;
