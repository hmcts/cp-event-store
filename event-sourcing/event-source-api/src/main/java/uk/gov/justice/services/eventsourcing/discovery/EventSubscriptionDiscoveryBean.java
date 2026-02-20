package uk.gov.justice.services.eventsourcing.discovery;

import static javax.ejb.TransactionAttributeType.REQUIRES_NEW;
import static javax.ejb.TransactionManagementType.CONTAINER;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;

@Stateless
@TransactionManagement(CONTAINER)
public class EventSubscriptionDiscoveryBean {

    @Inject
    private EventDiscoveryRepository eventDiscoveryRepository;

    @Inject
    private EventDiscoveryConfig eventDiscoveryConfig;

    @TransactionAttribute(REQUIRES_NEW)
    public DiscoveryResult discoverNewEvents(final long firstEventNumber, final UUID latestKnownEventId) {

        final int batchSize = eventDiscoveryConfig.getBatchSize();

        return eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(firstEventNumber, batchSize)
                .filter(newLatestEvent -> !newLatestEvent.id().equals(latestKnownEventId))
                .map(newLatestEvent -> {
                    final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositionsBetween(
                            firstEventNumber,
                            newLatestEvent.eventNumber());
                    return new DiscoveryResult(streamPositions, Optional.of(newLatestEvent.id()));
                })
                .orElseGet(() -> new DiscoveryResult(Collections.emptyList(), Optional.empty()));
    }
}
