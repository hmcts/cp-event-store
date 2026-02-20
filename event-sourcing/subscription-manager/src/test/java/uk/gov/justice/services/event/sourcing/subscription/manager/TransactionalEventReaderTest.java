package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionalEventReaderTest {

    @Mock
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private TransactionalEventReader transactionalEventReader;

    @Test
    public void shouldReadNextEventAndReturnJsonEnvelope() {

        final String source = "some-source";
        final UUID streamId = randomUUID();
        final long position = 5L;

        final LinkedEventSource linkedEventSource = org.mockito.Mockito.mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = org.mockito.Mockito.mock(LinkedEvent.class);
        final JsonEnvelope jsonEnvelope = org.mockito.Mockito.mock(JsonEnvelope.class);

        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, position)).thenReturn(of(linkedEvent));
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(jsonEnvelope);

        assertThat(transactionalEventReader.readNextEvent(source, streamId, position), is(of(jsonEnvelope)));
    }

    @Test
    public void shouldReturnEmptyWhenNoLinkedEventFound() {

        final String source = "some-source";
        final UUID streamId = randomUUID();
        final long position = 5L;

        final LinkedEventSource linkedEventSource = org.mockito.Mockito.mock(LinkedEventSource.class);

        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, position)).thenReturn(empty());

        assertThat(transactionalEventReader.readNextEvent(source, streamId, position), is(empty()));
    }
}