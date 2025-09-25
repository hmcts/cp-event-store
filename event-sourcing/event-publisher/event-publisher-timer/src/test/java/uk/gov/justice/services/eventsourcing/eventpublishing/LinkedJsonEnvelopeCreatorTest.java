package uk.gov.justice.services.eventsourcing.eventpublishing;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.spi.DefaultEnvelopeProvider;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.justice.services.test.utils.framework.api.JsonObjectConvertersFactory;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedJsonEnvelopeCreatorTest {

    @Spy
    private MetadataEventNumberUpdater metadataEventNumberUpdater = new MetadataEventNumberUpdater();

    @Spy
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider = new DefaultJsonEnvelopeProvider();

    @Spy
    private StringToJsonObjectConverter stringToJsonObjectConverter = new JsonObjectConvertersFactory().stringToJsonObjectConverter();

    @InjectMocks
    private LinkedJsonEnvelopeCreator linkedJsonEnvelopeCreator;

    @BeforeEach
    public void setupMetadataEventNumberUpdater() {
        setField(metadataEventNumberUpdater, "defaultEnvelopeProvider", new DefaultEnvelopeProvider());
    }

    @Test
    public void shouldCreateValidJsonEnvelopeWithEventNumbersFromLinkedEvent() throws Exception {

        final UUID eventId = randomUUID();
        final String eventName = "some-event-name";
        final Metadata metadata = metadataBuilder()
                .withName(eventName)
                .withId(eventId)
                .build();

        final String metadataJson = metadata.asJsonObject().toString();

        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;

        final String payloadJson = """
                {"payloadField_name_1":"payloadField_value_1"}
                """;

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(linkedEvent.getMetadata()).thenReturn(metadataJson);
        when(linkedEvent.getEventNumber()).thenReturn(of(eventNumber));
        when(linkedEvent.getPreviousEventNumber()).thenReturn(previousEventNumber);
        when(linkedEvent.getPayload()).thenReturn(payloadJson);

        final JsonEnvelope linkedJsonEnvelope = linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent);

        final String envelopJsonAsString = linkedJsonEnvelope.asJsonObject().toString();

        with(envelopJsonAsString)
                .assertThat("$.payloadField_name_1", is("payloadField_value_1"))
                .assertThat("$._metadata.name", is(eventName))
                .assertThat("$._metadata.id", is(eventId.toString()))
                .assertThat("$._metadata.event.eventNumber", is(eventNumber.intValue()))
                .assertThat("$._metadata.event.previousEventNumber", is(previousEventNumber.intValue()))
                ;
    }

    @Test
    public void shouldThrowMissingEventNumberExceptionIfLinkedEventHasMissingEventNumber() throws Exception {

        final UUID eventId = fromString("4ccded9c-7238-4194-b952-558ec054df40");
        final String eventName = "some-event-name";
        final Metadata metadata = metadataBuilder()
                .withName(eventName)
                .withId(eventId)
                .build();

        final String metadataJson = metadata.asJsonObject().toString();

        final Long previousEventNumber = 22L;

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(linkedEvent.getMetadata()).thenReturn(metadataJson);
        when(linkedEvent.getId()).thenReturn(eventId);
        when(linkedEvent.getEventNumber()).thenReturn(empty());
        when(linkedEvent.getPreviousEventNumber()).thenReturn(previousEventNumber);

        final MissingEventNumberException missingEventNumberException = assertThrows(
                MissingEventNumberException.class,
                () -> linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent));

        assertThat(missingEventNumberException.getMessage(), is("Linked event with eventId '4ccded9c-7238-4194-b952-558ec054df40' from event_log table has null event_number"));
    }
}