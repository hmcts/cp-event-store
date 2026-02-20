package uk.gov.justice.services.eventsourcing.source.core;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourceEventRepository;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public class DefaultLinkedEventSource implements LinkedEventSource {

    private final MultipleDataSourceEventRepository multipleDataSourceEventRepository;

    public DefaultLinkedEventSource(final MultipleDataSourceEventRepository multipleDataSourceEventRepository) {
        this.multipleDataSourceEventRepository = multipleDataSourceEventRepository;
    }

    @Override
    public Stream<LinkedEvent> findEventsSince(final long eventNumber) {
        return multipleDataSourceEventRepository.findEventsSince(eventNumber);
    }

    @Override
    public Stream<LinkedEvent> findEventRange(final MissingEventRange missingEventRange) {

        final Long fromEventNumber = missingEventRange.getMissingEventFrom();
        final Long toEventNumber = missingEventRange.getMissingEventTo();

        return multipleDataSourceEventRepository.findEventRange(fromEventNumber, toEventNumber);
    }

    @Override
    public Optional<LinkedEvent> findByEventId(final UUID eventId) {
        return multipleDataSourceEventRepository.findByEventId(eventId);
    }

    @Override
    public Optional<LinkedEvent> findNextEventInTheStreamAfterPosition(final UUID streamId, final Long position) {
        return multipleDataSourceEventRepository.findNextEventInTheStreamAfterPosition(streamId, position);
    }

    @Override
    public Long getHighestPublishedEventNumber() {
        final Optional<LinkedEvent> latestPublishedEvent = multipleDataSourceEventRepository
                .getLatestLinkedEvent();

        return latestPublishedEvent.map(publishedEvent -> publishedEvent
                        .getEventNumber()
                        .orElse(0L))
                .orElse(0L);
    }
}
