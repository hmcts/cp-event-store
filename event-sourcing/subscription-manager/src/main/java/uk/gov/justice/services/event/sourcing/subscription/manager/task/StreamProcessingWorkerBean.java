package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static jakarta.ejb.TransactionAttributeType.NEVER;
import static jakarta.ejb.TransactionManagementType.CONTAINER;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;

import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamEventProcessor;

import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionManagement;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.slf4j.Logger;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(NEVER)
public class StreamProcessingWorkerBean {

    @Inject
    private StreamEventProcessor streamEventProcessor;

    @Inject
    private PollerCircuitBreaker pollerCircuitBreaker;

    @Inject
    private Logger logger;

    @Transactional(Transactional.TxType.NEVER)
    public void processUntilIdle(final String source, final String component) {
        if (pollerCircuitBreaker.isOpen(source, component)) {
            logger.warn("Circuit breaker open, skipping processing for source: {}, component: {}", source, component);
            return;
        }

        try {
            EventProcessingStatus status;
            do {
                status = streamEventProcessor.processSingleEvent(source, component);
            } while (status == EVENT_FOUND);
            pollerCircuitBreaker.recordSuccess(source, component);
        } catch (final Exception e) {
            pollerCircuitBreaker.recordFailure(source, component);
            logger.error("Error processing stream events for source: {}, component: {}", source, component, e);
        }
    }
}
