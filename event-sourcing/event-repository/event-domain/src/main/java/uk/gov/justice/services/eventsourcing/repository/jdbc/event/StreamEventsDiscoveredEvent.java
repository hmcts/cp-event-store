package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

public record StreamEventsDiscoveredEvent(String source, String component) {

}