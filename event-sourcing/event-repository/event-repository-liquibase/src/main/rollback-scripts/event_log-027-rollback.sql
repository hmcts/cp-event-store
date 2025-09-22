-- Roll back liquibase script '027-drop-pre-publish-queue-table.changelog.xml'

CREATE TABLE pre_publish_queue (
   event_log_id uuid PRIMARY KEY,
   date_queued timestamp with time zone NOT NULL
);

CREATE UNIQUE INDEX pre_publish_queue_pkey ON pre_publish_queue(event_log_id);
