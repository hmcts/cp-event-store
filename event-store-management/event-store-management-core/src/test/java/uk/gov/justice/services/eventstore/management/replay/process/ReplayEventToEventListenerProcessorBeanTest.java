package uk.gov.justice.services.eventstore.management.replay.process;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.LinkedEventMetadataUpdater;
import uk.gov.justice.services.event.sourcing.subscription.manager.LinkedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ReplayEventToEventListenerProcessorBeanTest {

    private static final UUID COMMAND_ID = randomUUID();
    private static final UUID COMMAND_RUNTIME_ID = randomUUID();
    private static final String EVENT_SOURCE_NAME = "listenerEventSourceName";

    private static final ReplayEventContext REPLAY_EVENT_CONTEXT = new ReplayEventContext(COMMAND_ID, COMMAND_RUNTIME_ID, EVENT_SOURCE_NAME, EVENT_LISTENER);

    @Mock
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Mock
    private TransactionReplayEventProcessor transactionReplayEventProcessor;

    @Mock
    private EventConverter eventConverter;

    @Mock
    private LinkedEventMetadataUpdater linkedEventMetadataUpdater;

    @InjectMocks
    private ReplayEventToEventListenerProcessorBean replayEventToEventListenerProcessorBean;

    @Test
    public void shouldFetchLinkedEventAndInvokeEventProcessor() {
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final LinkedEvent updatedLinkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope eventEnvelope = mock(JsonEnvelope.class);

        when(linkedEventSourceProvider.getLinkedEventSource(EVENT_SOURCE_NAME)).thenReturn(linkedEventSource);
        when(linkedEventSource.findByEventId(COMMAND_RUNTIME_ID)).thenReturn(of(linkedEvent));
        when(linkedEventMetadataUpdater.addEventNumbersToMetadataOf(linkedEvent)).thenReturn(updatedLinkedEvent);
        when(eventConverter.envelopeOf(updatedLinkedEvent)).thenReturn(eventEnvelope);

        replayEventToEventListenerProcessorBean.perform(REPLAY_EVENT_CONTEXT);

        verify(transactionReplayEventProcessor).process(EVENT_SOURCE_NAME, EVENT_LISTENER, eventEnvelope);
    }

    @Test
    public void shouldThrowExceptionWhenPublishedEventFetchFails() {
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        when(linkedEventSourceProvider.getLinkedEventSource(EVENT_SOURCE_NAME)).thenReturn(linkedEventSource);
        when(linkedEventSource.findByEventId(COMMAND_RUNTIME_ID)).thenReturn(empty());

        final ReplayEventFailedException e = assertThrows(ReplayEventFailedException.class, () -> replayEventToEventListenerProcessorBean.perform(REPLAY_EVENT_CONTEXT));

        assertThat(e.getMessage(), is("Event not found for eventId: " + COMMAND_RUNTIME_ID + " from event source name: " + EVENT_SOURCE_NAME));
    }
}