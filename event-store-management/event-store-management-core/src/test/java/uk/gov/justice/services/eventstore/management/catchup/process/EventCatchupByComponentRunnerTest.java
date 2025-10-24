package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventCatchupByComponentRunnerTest {

    @Mock
    private EventCatchupProcessorBean eventCatchupProcessorBean;

    @Mock
    private RunFromEventNumberFinder runFromEventNumberFinder;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventCatchupByComponentRunner eventCatchupByComponentRunner;

    @Test
    public void shouldGetAllSubscriptionsForTheComponentAndRunCatchupOnEach() throws Exception {

        final UUID commandId = randomUUID();
        final String componentName = "AN_EVENT_LISTENER";
        final String subscriptionName = "subscriptionName";
        final Optional<UUID> runFromEventId = of(randomUUID());
        final long runFromEventNumber = 23L;

        final SubscriptionCatchupDetails subscriptionCatchupDefinition = mock(SubscriptionCatchupDetails.class);

        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(subscriptionCatchupDefinition.getComponentName()).thenReturn(componentName);
        when(subscriptionCatchupDefinition.getSubscriptionName()).thenReturn(subscriptionName);
        when(runFromEventNumberFinder.findEventNumberToRunFrom(runFromEventId, subscriptionName)).thenReturn(runFromEventNumber);

        eventCatchupByComponentRunner.runEventCatchupForComponent(
                subscriptionCatchupDefinition,
                commandId,
                eventCatchupCommand,
                runFromEventId);

        final InOrder inOrder = inOrder(logger, eventCatchupProcessorBean);

        inOrder.verify(logger).info("Running CATCHUP for Component 'AN_EVENT_LISTENER', Subscription 'subscriptionName' from event number '23'");
        inOrder.verify(eventCatchupProcessorBean).performEventCatchup(new CatchupSubscriptionContext(
                commandId,
                componentName,
                subscriptionCatchupDefinition,
                eventCatchupCommand,
                runFromEventNumber));
    }
}
