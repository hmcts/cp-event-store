package uk.gov.justice.services.eventsourcing.discovery;

import static javax.ejb.TransactionManagementType.CONTAINER;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;
import javax.transaction.Transactional;

@Stateless
@TransactionManagement(CONTAINER)
public class EventSubscriptionDiscoveryBean {

    private static final long ZEROTH_EVENT_NUMBER = 0L;

    @Inject
    private EventDiscoveryRepository eventDiscoveryRepository;

    @Inject
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Transactional(REQUIRES_NEW)
    public DiscoveryResult discoverNewEvents(final Optional<UUID> latestKnownEventId) {

        final int batchSize = eventDiscoveryConfig.getBatchSize();

        final long firstEventNumber = latestKnownEventId
                .map(eventDiscoveryRepository::getEventNumberFor)
                .orElse(ZEROTH_EVENT_NUMBER);

        return eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(firstEventNumber, batchSize)
                .filter(newLatestEvent -> !Objects.equals(newLatestEvent.id(),latestKnownEventId.orElse(null)))
                .map(newLatestEvent -> {
                    final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositionsBetween(
                            firstEventNumber,
                            newLatestEvent.eventNumber());
                    return new DiscoveryResult(streamPositions, Optional.of(newLatestEvent.id()));
                })
                .orElseGet(() -> new DiscoveryResult(Collections.emptyList(), Optional.empty()));
    }
}
