package uk.gov.justice.services.eventsourcing.discovery;

import static javax.ejb.TransactionManagementType.CONTAINER;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.List;
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
    public List<StreamPosition> discoverNewEvents(final Optional<UUID> latestKnownEventId) {

        final int batchSize = eventDiscoveryConfig.getBatchSize();
        if(latestKnownEventId.isPresent()) {
            return eventDiscoveryRepository.getLatestStreamPositions(
                    latestKnownEventId.get(),
                    batchSize);
        }

        return eventDiscoveryRepository.getLatestStreamPositions(ZEROTH_EVENT_NUMBER, batchSize);
    }
}
