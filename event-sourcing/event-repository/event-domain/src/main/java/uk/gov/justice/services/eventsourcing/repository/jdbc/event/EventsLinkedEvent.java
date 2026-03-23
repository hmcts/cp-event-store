package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import java.util.List;

public record EventsLinkedEvent(List<StreamPosition> streamPositions) {

}