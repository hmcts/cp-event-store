package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static javax.ejb.TransactionAttributeType.NEVER;
import static javax.ejb.TransactionManagementType.CONTAINER;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;

import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamEventProcessor;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(NEVER)
public class StreamProcessingWorkerBean {

    @Inject
    private StreamEventProcessor streamEventProcessor;

    @Inject
    private Logger logger;

    @Transactional(Transactional.TxType.NEVER)
    public boolean processUntilIdle(final String source, final String component) {
        boolean anyEventFound = false;

        try {
            EventProcessingStatus status;
            do {
                status = streamEventProcessor.processSingleEvent(source, component);
                if (status == EVENT_FOUND) {
                    anyEventFound = true;
                }
            } while (status == EVENT_FOUND);
        } catch (final StreamProcessingException e) {
            logger.warn("Stream has pending events not yet available for source: {}, component: {}: {}",
                    source, component, e.getMessage());
            return true;
        } catch (final Exception e) {
            logger.error("Error processing stream events for source: {}, component: {}", source, component, e);
        }

        return anyEventFound;
    }
}
