package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventStreamConsumptionResolver;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupProcessingOfEventFailedEvent;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumeEventQueueBeanTest {

    @Mock
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    @Mock
    private Event<CatchupProcessingOfEventFailedEvent> catchupProcessingOfEventFailedEventFirer;

    @Mock
    private EventStreamConsumptionResolver eventStreamConsumptionResolver;

    @Mock
    private EventQueueConsumer eventQueueConsumer;

    @InjectMocks
    private ConsumeEventQueueBean consumeEventQueueBean;

    @Test
    public void shouldConsumeTheEventQueueUntilEventsConsumedIsTrue() throws Exception {

        final UUID commandId = randomUUID();
        final Queue<LinkedEvent> events = new ConcurrentLinkedQueue<>(singletonList(mock(LinkedEvent.class)));
        final String subscriptionName = "subscriptionName";
        final CatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(eventQueueConsumer.consumeEventQueue(commandId, events, subscriptionName, eventCatchupCommand)).thenReturn(false, false, true);

        consumeEventQueueBean.consume(
                events,
                subscriptionName,
                eventCatchupCommand,
                commandId
        );

        verify(eventQueueConsumer, times(3)).consumeEventQueue(commandId, events, subscriptionName, eventCatchupCommand);
    }

    @Test
    public void shouldCatchTheHiddenExceptionAndClearTheEventQueueIfTheTransactionFails() throws Exception {

        final RuntimeException runtimeException = new RuntimeException(
                "In reality this will be a jakarta.transaction.RollbackException"
        );

        final UUID commandId = randomUUID();
        final Queue<LinkedEvent> events = new ConcurrentLinkedQueue<>(singletonList(mock(LinkedEvent.class)));
        final String subscriptionName = "subscriptionName";
        final CatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(eventQueueConsumer.consumeEventQueue(commandId, events, subscriptionName, eventCatchupCommand))
                .thenThrow(runtimeException)
                .thenReturn(true);

        assertThat(events.isEmpty(), is(false));

        consumeEventQueueBean.consume(
                events,
                subscriptionName,
                eventCatchupCommand,
                commandId
        );

        verify(eventStreamConsumptionResolver).decrementEventsInProcessCountBy(1);
        verify(eventQueueConsumer, times(2)).consumeEventQueue(commandId, events, subscriptionName, eventCatchupCommand);
        verify(eventProcessingFailedHandler).handleStreamFailure(runtimeException, subscriptionName, eventCatchupCommand, commandId);
        assertThat(events.isEmpty(), is(true));
    }
}
