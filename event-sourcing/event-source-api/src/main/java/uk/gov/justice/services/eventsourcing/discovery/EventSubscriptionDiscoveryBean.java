package uk.gov.justice.services.eventsourcing.discovery;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static java.util.UUID.randomUUID;
import static javax.ejb.TransactionManagementType.CONTAINER;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;
import javax.transaction.Transactional;

@Stateless
@TransactionManagement(CONTAINER)
public class EventSubscriptionDiscoveryBean {

    @Inject
    private EventDiscoveryRepository eventDiscoveryRepository;

    @Inject
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Transactional(REQUIRES_NEW)
    public List<StreamPosition> discoverNewEvents(final UUID latestKnownEventId) {

        return eventDiscoveryRepository.getLatestStreamPositions(
                latestKnownEventId,
                eventDiscoveryConfig.getBatchSize());
    }
}
