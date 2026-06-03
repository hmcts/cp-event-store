package uk.gov.justice.services.eventstore.management.catchup.process;

import static jakarta.ejb.TransactionAttributeType.NEVER;
import static jakarta.ejb.TransactionManagementType.CONTAINER;

import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionManagement;
import jakarta.inject.Inject;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(value = NEVER)
public class EventCatchupProcessorBean {

    @Inject
    private EventCatchupProcessor eventCatchupProcessor;

    public void performEventCatchup(final CatchupSubscriptionContext catchupSubscriptionContext) {

        eventCatchupProcessor.performEventCatchup(catchupSubscriptionContext);
    }
}
