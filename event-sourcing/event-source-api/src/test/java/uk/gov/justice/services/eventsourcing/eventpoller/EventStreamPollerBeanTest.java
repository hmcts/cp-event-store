package uk.gov.justice.services.eventsourcing.eventpoller;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;


@ExtendWith(MockitoExtension.class)
public class EventStreamPollerBeanTest {

    @Mock
    private LinkedEventSource linkedEventSource;

    @Mock
    private EventStreamPollerConfig eventStreamPollerConfig;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private EventStreamPollerBean eventStreamPollerBean;

    @Test
    public void shouldPollStreamEventsWithCorrectParameters() {
        final UUID streamId = UUID.randomUUID();
        final long fromPosition = 1L;
        final long toPosition = 5L;
        final int batchSize = 10;

        final LinkedEvent linkedEvent1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent3 = mock(LinkedEvent.class);
        final Stream<LinkedEvent> streamOfEvents = Stream.of(linkedEvent1, linkedEvent2, linkedEvent3);

        final JsonEnvelope envelope1 = mock(JsonEnvelope.class);
        final JsonEnvelope envelope2 = mock(JsonEnvelope.class);
        final JsonEnvelope envelope3 = mock(JsonEnvelope.class);


        when(eventConverter.envelopeOf(linkedEvent1)).thenReturn(envelope1);
        when(eventConverter.envelopeOf(linkedEvent2)).thenReturn(envelope2);
        when(eventConverter.envelopeOf(linkedEvent3)).thenReturn(envelope3);

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(linkedEventSource.pollStreamEvents(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(streamOfEvents);

        final List<JsonEnvelope> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);


        verify(eventStreamPollerConfig).getBatchSize();
        verify(linkedEventSource).pollStreamEvents(streamId, fromPosition, toPosition, batchSize);

        assertThat(result.size(), is(3));
        assertThat(result.get(0), is(envelope1));
        assertThat(result.get(1), is(envelope2));
        assertThat(result.get(2), is(envelope3));

    }


    @Test
    public void shouldHandleEmptyStream() {

        final UUID streamId = UUID.randomUUID();
        final long fromPosition = 5L;
        final long toPosition = 10L;
        final int batchSize = 25;

        final Stream<LinkedEvent> emptyStream = Stream.empty();

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(linkedEventSource.pollStreamEvents(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(emptyStream);

        final List<JsonEnvelope> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);

        assertThat(result.isEmpty(), is(true));
    }

}

