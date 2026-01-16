package uk.gov.justice.services.eventsourcing.eventpoller;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcBasedEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjects;

import javax.json.Json;

@ExtendWith(MockitoExtension.class)
public class EventStreamPollerBeanTest {

    @Mock
    private JdbcBasedEventRepository jdbcBasedEventRepository;

    @Mock
    private EventStreamPollerConfig eventStreamPollerConfig;

    @InjectMocks
    private EventStreamPollerBean eventStreamPollerBean;

    @Test
    public void shouldPollStreamEventsWithCorrectParameters() {
        final UUID streamId = UUID.randomUUID();
        final long fromPosition = 1L;
        final long toPosition = 5L;
        final int batchSize = 10;

        final JsonEnvelope envelope1 = createEnvelope(streamId, 1L);
        final JsonEnvelope envelope2 = createEnvelope(streamId, 2L);
        final JsonEnvelope envelope3 = createEnvelope(streamId, 3L);
        final Stream<JsonEnvelope> expectedStream = Stream.of(envelope1, envelope2, envelope3);

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(jdbcBasedEventRepository.pollStreamEvents(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(expectedStream);

        final List<JsonEnvelope> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);


        verify(eventStreamPollerConfig).getBatchSize();
        verify(jdbcBasedEventRepository).pollStreamEvents(streamId, fromPosition, toPosition, batchSize);

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

        final Stream<JsonEnvelope> emptyStream = Stream.empty();

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(jdbcBasedEventRepository.pollStreamEvents(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(emptyStream);

        final List<JsonEnvelope> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);

        assertThat(result.isEmpty(), is(true));
    }


    private JsonEnvelope createEnvelope(final UUID streamId, final Long positionInStream) {
        return JsonEnvelope.envelopeFrom(JsonEnvelope.metadataBuilder()
                .withStreamId(streamId)
                .withId(randomUUID())
                .withPosition(positionInStream)
                .withName("eventA"), JsonObjects.createObjectBuilder());
    }
}

