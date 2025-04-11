package uk.gov.justice.services.event.sourcing.subscription.manager;

public enum EventProcessingStatus {

    EVENT_IS_OBSOLETE,
    SHOULD_BUFFER_EVENT,
    SHOULD_PROCESS_EVENT
}
