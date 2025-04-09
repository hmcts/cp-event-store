package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class EventBufferProcessorTest {

    private final String component = "some-component";

    private final EventBufferService eventBufferService = mock(EventBufferService.class);
    private final SubscriptionEventProcessor subscriptionEventProcessor = mock(SubscriptionEventProcessor.class);

    private EventBufferProcessor eventBufferProcessor = new EventBufferProcessor(eventBufferService, subscriptionEventProcessor, component);

    @Test
    public void shouldProcessAllEventsOnTheEventBuffer() throws Exception {


        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class, "incomingJsonEnvelope");
        final JsonEnvelope eventEnvelope_2 = mock(JsonEnvelope.class);
        final JsonEnvelope eventEnvelope_3 = mock(JsonEnvelope.class);

        final StreamWasClosedIndicator streamWasClosedIndicator = new StreamWasClosedIndicator();

        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(incomingJsonEnvelope, eventEnvelope_2, eventEnvelope_3);

        jsonEnvelopeStream.onClose(streamWasClosedIndicator::setClosed);

        assertThat(streamWasClosedIndicator.streamWasClosed(), is(false));

        when(eventBufferService.currentOrderedEventsWith(incomingJsonEnvelope, component)).thenReturn(jsonEnvelopeStream);
        eventBufferProcessor.processWithEventBuffer(incomingJsonEnvelope);

        verify(subscriptionEventProcessor).processSingleEvent(incomingJsonEnvelope, component);
        verify(subscriptionEventProcessor).processSingleEvent(eventEnvelope_2, component);
        verify(subscriptionEventProcessor).processSingleEvent(eventEnvelope_3, component);

        assertThat(streamWasClosedIndicator.streamWasClosed(), is(true));
    }

    private class StreamWasClosedIndicator {
        private boolean closed = false;

        boolean streamWasClosed() {
            return closed;
        }

        void setClosed() {
            closed = true;
        }
    }
}
