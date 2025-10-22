package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.ConcurrentEventStreamConsumerManager;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupCompletedForSubscriptionEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import javax.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventCatchupProcessorTest {

    @Mock
    private ConcurrentEventStreamConsumerManager concurrentEventStreamConsumerManager;

    @Mock
    private MissingEventStreamer missingEventStreamer;

    @Mock
    private Event<CatchupCompletedForSubscriptionEvent> catchupCompletedForSubscriptionEventFirer;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventCatchupProcessor eventCatchupProcessor;

    @Test
    public void shouldFetchAllMissingEventsAndProcess() throws Exception {

        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final String eventSourceName = "example.event.source";
        final String componentName = "EVENT_LISTENER";
        final long runFromEventNumber = 999L;

        final ZonedDateTime catchupStartedAt = new UtcClock().now();
        final ZonedDateTime catchupCompletedAt = catchupStartedAt.plusMinutes(23);

        final SubscriptionCatchupDetails subscriptionCatchupDetails = mock(SubscriptionCatchupDetails.class);
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final CatchupSubscriptionContext catchupSubscriptionContext = new CatchupSubscriptionContext(
                commandId,
                componentName,
                subscriptionCatchupDetails,
                catchupCommand,
                runFromEventNumber);

        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final List<LinkedEvent> events = asList(linkedEvent_1, linkedEvent_2, linkedEvent_3);

        when(missingEventStreamer.getMissingEvents(eventSourceName, componentName, runFromEventNumber)).thenReturn(events.stream());

        when(linkedEvent_1.getEventNumber()).thenReturn(of(runFromEventNumber + 1L));
        when(linkedEvent_2.getEventNumber()).thenReturn(of(runFromEventNumber + 2L));
        when(linkedEvent_3.getEventNumber()).thenReturn(of(runFromEventNumber + 3L));

        when(subscriptionCatchupDetails.getSubscriptionName()).thenReturn(subscriptionName);
        when(subscriptionCatchupDetails.getEventSourceName()).thenReturn(eventSourceName);
        when(clock.now()).thenReturn(catchupCompletedAt);

        when(concurrentEventStreamConsumerManager.add(linkedEvent_1, subscriptionName, catchupCommand, commandId)).thenReturn(1);
        when(concurrentEventStreamConsumerManager.add(linkedEvent_2, subscriptionName, catchupCommand, commandId)).thenReturn(1);
        when(concurrentEventStreamConsumerManager.add(linkedEvent_3, subscriptionName, catchupCommand, commandId)).thenReturn(1);

        eventCatchupProcessor.performEventCatchup(catchupSubscriptionContext);

        final InOrder inOrder = inOrder(concurrentEventStreamConsumerManager, catchupCompletedForSubscriptionEventFirer);

        inOrder.verify(concurrentEventStreamConsumerManager).add(linkedEvent_1, subscriptionName, catchupCommand, commandId);
        inOrder.verify(concurrentEventStreamConsumerManager).add(linkedEvent_2, subscriptionName, catchupCommand, commandId);
        inOrder.verify(concurrentEventStreamConsumerManager).add(linkedEvent_3, subscriptionName, catchupCommand, commandId);
        inOrder.verify(concurrentEventStreamConsumerManager).waitForCompletion();

        inOrder.verify(catchupCompletedForSubscriptionEventFirer).fire(new CatchupCompletedForSubscriptionEvent(
                commandId,
                subscriptionName,
                eventSourceName,
                componentName,
                catchupCommand,
                catchupCompletedAt,
                events.size()));

        verify(logger).info("Finding all missing events for event source 'example.event.source', component 'EVENT_LISTENER");
        verify(logger).info("CATCHUP with Event Source: example.event.source for Event Number: " + (runFromEventNumber + 1L));
        verify(logger).info("3 active PublishedEvents queued for publishing");
        verify(logger).info("Waiting for publishing consumer completion...");
    }

    @Test
    public void shouldThrowExceptionIfEventNumberIsAbsentFromPublishedEvent() throws Exception {

        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final String eventSourceName = "event source";
        final String componentName = "EVENT_LISTENER";
        final long runFromEventNumber = 23L;
        final UUID idOfEventWithNoEventNumber = fromString("937f9fd6-3679-4bc2-a73c-6a7b18a651e1");

        final SubscriptionCatchupDetails subscriptionCatchupDetails = mock(SubscriptionCatchupDetails.class);
        final CatchupCommand catchupCommand = mock(CatchupCommand.class);

        final CatchupSubscriptionContext catchupSubscriptionContext = new CatchupSubscriptionContext(
                commandId,
                componentName,
                subscriptionCatchupDetails,
                catchupCommand,
                runFromEventNumber);

        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final List<LinkedEvent> events = asList(linkedEvent_1, linkedEvent_2, linkedEvent_3);

        when(missingEventStreamer.getMissingEvents(eventSourceName, componentName, runFromEventNumber)).thenReturn(events.stream());

        when(linkedEvent_1.getEventNumber()).thenReturn(of(runFromEventNumber + 1));
        when(linkedEvent_2.getEventNumber()).thenReturn(of(runFromEventNumber + 2));
        when(linkedEvent_3.getEventNumber()).thenReturn(empty());
        when(linkedEvent_3.getId()).thenReturn(idOfEventWithNoEventNumber);

        when(subscriptionCatchupDetails.getSubscriptionName()).thenReturn(subscriptionName);
        when(subscriptionCatchupDetails.getEventSourceName()).thenReturn(eventSourceName);
        when(concurrentEventStreamConsumerManager.add(linkedEvent_1, subscriptionName, catchupCommand, commandId)).thenReturn(1);
        when(concurrentEventStreamConsumerManager.add(linkedEvent_2, subscriptionName, catchupCommand, commandId)).thenReturn(1);

        try {
            eventCatchupProcessor.performEventCatchup(catchupSubscriptionContext);
            fail();
        } catch (final MissingEventNumberException expected) {
            assertThat(expected.getMessage(), is("Event with id '937f9fd6-3679-4bc2-a73c-6a7b18a651e1' is missing its event number"));
        }
    }
}
