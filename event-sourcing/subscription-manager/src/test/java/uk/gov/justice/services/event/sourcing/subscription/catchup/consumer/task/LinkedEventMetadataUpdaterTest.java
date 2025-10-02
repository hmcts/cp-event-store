package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.MetadataBuilder;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventMetadataUpdaterTest {

    @Mock
    private MetadataEventNumberUpdater metadataEventNumberUpdater;

    @Mock
    private StringToJsonObjectConverter stringToJsonObjectConverter;

    @Mock
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;

    @InjectMocks
    private LinkedEventMetadataUpdater linkedEventMetadataUpdater;

    @Test
    public void shouldAddEventNumbersToMetadataOfLinkedEvent() throws Exception {

        final UUID id = randomUUID();
        final UUID streamId = randomUUID();
        final Long positionInStream = 23L;
        final String name = "some-event-name";
        final String metadata = "original-metadata";
        final String payload = "event-payload";
        final ZonedDateTime createdAt = new UtcClock().now();
        final Long eventNumber = 42L;
        final Long previousEventNumber = 41L;

        final String metadataWithEventNumbersJson = "updated-metadata";

        final LinkedEvent linkedEvent = new LinkedEvent(
                id,
                streamId,
                positionInStream,
                name,
                metadata,
                payload,
                createdAt,
                eventNumber,
                previousEventNumber
        );

        final JsonObject originalMetadataJsonObject = mock(JsonObject.class);
        final Metadata originalMetadata = mock(Metadata.class);
        final Metadata metadataWithEventNumbers = mock(Metadata.class);
        final MetadataBuilder metadataBuilder = mock(MetadataBuilder.class);
        final JsonObject metadataWithEventNumbersJsonObject = mock(JsonObject.class);

        when(stringToJsonObjectConverter.convert(linkedEvent.getMetadata())).thenReturn(originalMetadataJsonObject);
        when(defaultJsonEnvelopeProvider.metadataFrom(originalMetadataJsonObject)).thenReturn(metadataBuilder);
        when(metadataBuilder.build()).thenReturn(originalMetadata);
        when(metadataEventNumberUpdater.updateMetadataJson(
                originalMetadata,
                previousEventNumber,
                eventNumber)).thenReturn(metadataWithEventNumbers);
        when(metadataWithEventNumbers.asJsonObject()).thenReturn(metadataWithEventNumbersJsonObject);
        when(metadataWithEventNumbersJsonObject.toString()).thenReturn(metadataWithEventNumbersJson);

        final LinkedEvent updatedLinkedEvent = linkedEventMetadataUpdater.addEventNumbersToMetadataOf(linkedEvent);

        assertThat(updatedLinkedEvent.getId(), is(id));
        assertThat(updatedLinkedEvent.getStreamId(), is(streamId));
        assertThat(updatedLinkedEvent.getPositionInStream(), is(positionInStream));
        assertThat(updatedLinkedEvent.getName(), is(name));
        assertThat(updatedLinkedEvent.getMetadata(), is(metadataWithEventNumbersJson));
        assertThat(updatedLinkedEvent.getCreatedAt(), is(createdAt));
        assertThat(updatedLinkedEvent.getEventNumber(), is(of(eventNumber)));
        assertThat(updatedLinkedEvent.getPreviousEventNumber(), is(previousEventNumber));
    }

    @Test
    public void shouldThrowMissingEventNumberExceptionIfLinkedEventHasMissingEventNumber() throws Exception {

        final UUID eventId = fromString("632d31f9-bcbe-470d-82c2-4dd67ed6f6e5");

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        when(linkedEvent.getId()).thenReturn(eventId);
        when(linkedEvent.getEventNumber()).thenReturn(empty());

        final MissingEventNumberException missingEventNumberException = assertThrows(
                MissingEventNumberException.class,
                () -> linkedEventMetadataUpdater.addEventNumbersToMetadataOf(linkedEvent));

        assertThat(missingEventNumberException.getMessage(), is("Event with eventId '632d31f9-bcbe-470d-82c2-4dd67ed6f6e5' has no event number"));
    }
}