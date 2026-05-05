package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupProcessingOfEventFailedEvent;

import java.util.UUID;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventProcessingFailedHandlerTest {

    @Mock
    private Event<CatchupProcessingOfEventFailedEvent> catchupProcessingOfEventFailedEventFirer;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    @Captor
    private ArgumentCaptor<CatchupProcessingOfEventFailedEvent> catchupProcessingOfEventFailedEventCaptor;

    @Test
    public void shouldLogExceptionAndFireFailureEventOnEventFailure() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final String eventName = "events.some-event";
        final UUID eventId = fromString("1c68536e-1fdf-4891-9c74-85661a9c0f9e");
        final UUID streamId = fromString("b1834ed9-e084-41c7-ae7f-74f1227cc829");
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(linkedEvent.getName()).thenReturn(eventName);
        when(linkedEvent.getId()).thenReturn(eventId);
        when(linkedEvent.getStreamId()).thenReturn(streamId);

        eventProcessingFailedHandler.handleEventFailure(
                nullPointerException,
                linkedEvent,
                subscriptionName,
                catchupCommand,
                commandId);

        verify(logger).error("Failed to process publishedEvent: name: 'events.some-event', id: '1c68536e-1fdf-4891-9c74-85661a9c0f9e', streamId: 'b1834ed9-e084-41c7-ae7f-74f1227cc829'", nullPointerException);

        verify(catchupProcessingOfEventFailedEventFirer).fire(catchupProcessingOfEventFailedEventCaptor.capture());

        final CatchupProcessingOfEventFailedEvent catchupProcessingOfEventFailedEvent = catchupProcessingOfEventFailedEventCaptor.getValue();

        assertThat(catchupProcessingOfEventFailedEvent.getMessage(), is("Failed to process publishedEvent: name: 'events.some-event', id: '1c68536e-1fdf-4891-9c74-85661a9c0f9e', streamId: 'b1834ed9-e084-41c7-ae7f-74f1227cc829': NullPointerException: Ooops"));
        assertThat(catchupProcessingOfEventFailedEvent.getCatchupCommand(), is(catchupCommand));
        assertThat(catchupProcessingOfEventFailedEvent.getException(), is(nullPointerException));
        assertThat(catchupProcessingOfEventFailedEvent.getSubscriptionName(), is(subscriptionName));
    }

    @Test
    public void shouldLogExceptionAndFireFailureEventOnStreamFailure() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final String eventName = "events.some-event";
        final UUID eventId = fromString("1c68536e-1fdf-4891-9c74-85661a9c0f9e");
        final UUID streamId = fromString("b1834ed9-e084-41c7-ae7f-74f1227cc829");
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        eventProcessingFailedHandler.handleStreamFailure(
                nullPointerException,
                subscriptionName,
                catchupCommand,
                commandId);

        verify(logger).error("Failed to consume stream of events. Aborting...", nullPointerException);

        verify(catchupProcessingOfEventFailedEventFirer).fire(catchupProcessingOfEventFailedEventCaptor.capture());

        final CatchupProcessingOfEventFailedEvent catchupProcessingOfEventFailedEvent = catchupProcessingOfEventFailedEventCaptor.getValue();

        assertThat(catchupProcessingOfEventFailedEvent.getMessage(), is("Failed to consume stream of events. Aborting...: NullPointerException: Ooops"));
        assertThat(catchupProcessingOfEventFailedEvent.getCatchupCommand(), is(catchupCommand));
        assertThat(catchupProcessingOfEventFailedEvent.getException(), is(nullPointerException));
        assertThat(catchupProcessingOfEventFailedEvent.getSubscriptionName(), is(subscriptionName));
    }

    @Test
    public void shouldLogExceptionAndFireFailureEventOnSubscriptionFailure() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final UUID streamId = fromString("b1834ed9-e084-41c7-ae7f-74f1227cc829");
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        eventProcessingFailedHandler.handleSubscriptionFailure(
                nullPointerException,
                subscriptionName,
                commandId,
                catchupCommand);

        verify(logger).error("Failed to subscribe to 'subscriptionName'. Aborting...", nullPointerException);

        verify(catchupProcessingOfEventFailedEventFirer).fire(catchupProcessingOfEventFailedEventCaptor.capture());

        final CatchupProcessingOfEventFailedEvent catchupProcessingOfEventFailedEvent = catchupProcessingOfEventFailedEventCaptor.getValue();

        assertThat(catchupProcessingOfEventFailedEvent.getMessage(), is("Failed to subscribe to 'subscriptionName'. Aborting...: NullPointerException: Ooops"));
        assertThat(catchupProcessingOfEventFailedEvent.getCatchupCommand(), is(catchupCommand));
        assertThat(catchupProcessingOfEventFailedEvent.getException(), is(nullPointerException));
        assertThat(catchupProcessingOfEventFailedEvent.getSubscriptionName(), is(subscriptionName));
    }
}
