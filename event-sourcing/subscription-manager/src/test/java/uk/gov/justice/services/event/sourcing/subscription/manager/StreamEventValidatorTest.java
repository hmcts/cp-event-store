package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.of;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.core.json.JsonSchemaValidationException;
import uk.gov.justice.services.core.json.JsonSchemaValidator;
import uk.gov.justice.services.core.json.JsonValidationLoggerHelper;
import uk.gov.justice.services.core.mapping.MediaType;
import uk.gov.justice.services.core.mapping.NameToMediaTypeConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.subscription.SubscriptionEventNamesProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamEventValidatorTest {

    @Mock
    private Logger logger;

    @Mock
    private JsonSchemaValidator jsonSchemaValidator;

    @Mock
    private NameToMediaTypeConverter nameToMediaTypeConverter;

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Mock
    private JsonValidationLoggerHelper jsonValidationLoggerHelper;

    @Mock
    private SubscriptionEventNamesProvider subscriptionEventNamesProvider;

    @InjectMocks
    private StreamEventValidator streamEventValidator;

    @Test
    public void shouldValidateEventAgainstSchemaWhenEventNameIsAccepted() {

        final String source = "example";
        final String component = "EVENT_LISTENER";
        final String eventName = "example.recipe-added";
        final String envelopeJson = "{\"_metadata\":{\"name\":\"example.recipe-added\"},\"recipeId\":\"123\"}";
        final MediaType mediaType = mock(MediaType.class);

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(subscriptionEventNamesProvider.accepts(eventName, source, component)).thenReturn(true);
        when(nameToMediaTypeConverter.convert(eventName)).thenReturn(mediaType);
        when(jsonObjectEnvelopeConverter.asJsonString(jsonEnvelope)).thenReturn(envelopeJson);

        streamEventValidator.validate(jsonEnvelope, source, component);

        verify(jsonSchemaValidator).validate(envelopeJson, eventName, of(mediaType));
    }

    @Test
    public void shouldSkipValidationWhenEventNameIsNotAccepted() {

        final String source = "example";
        final String component = "EVENT_LISTENER";
        final String eventName = "example.unknown-event";

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(subscriptionEventNamesProvider.accepts(eventName, source, component)).thenReturn(false);

        streamEventValidator.validate(jsonEnvelope, source, component);

        verifyNoInteractions(jsonSchemaValidator);
        verifyNoInteractions(nameToMediaTypeConverter);
        verifyNoInteractions(jsonObjectEnvelopeConverter);
    }

    @Test
    public void shouldThrowJsonSchemaValidationExceptionWhenValidationFails() {

        final String source = "example";
        final String component = "EVENT_LISTENER";
        final String eventName = "example.recipe-added";
        final String envelopeJson = "{\"_metadata\":{\"name\":\"example.recipe-added\"},\"invalid\":true}";
        final MediaType mediaType = mock(MediaType.class);
        final JsonSchemaValidationException validationException = new JsonSchemaValidationException("Validation failed");

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(subscriptionEventNamesProvider.accepts(eventName, source, component)).thenReturn(true);
        when(nameToMediaTypeConverter.convert(eventName)).thenReturn(mediaType);
        when(jsonObjectEnvelopeConverter.asJsonString(jsonEnvelope)).thenReturn(envelopeJson);
        doThrow(validationException).when(jsonSchemaValidator).validate(envelopeJson, eventName, of(mediaType));
        when(jsonValidationLoggerHelper.toValidationTrace(validationException)).thenReturn("validation trace");

        assertThrows(JsonSchemaValidationException.class,
                () -> streamEventValidator.validate(jsonEnvelope, source, component));
    }
}
