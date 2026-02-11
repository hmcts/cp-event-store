package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static java.util.Optional.of;

import uk.gov.justice.services.core.json.JsonSchemaValidationException;
import uk.gov.justice.services.core.json.JsonSchemaValidator;
import uk.gov.justice.services.core.json.JsonValidationLoggerHelper;
import uk.gov.justice.services.core.mapping.MediaType;
import uk.gov.justice.services.core.mapping.NameToMediaTypeConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.subscription.SubscriptionEventNamesProvider;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamEventValidator {

    @Inject
    private Logger logger;

    @Inject
    private JsonSchemaValidator jsonSchemaValidator;

    @Inject
    private NameToMediaTypeConverter nameToMediaTypeConverter;

    @Inject
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    private JsonValidationLoggerHelper jsonValidationLoggerHelper;

    @Inject
    private SubscriptionEventNamesProvider subscriptionEventNamesProvider;

    public void validate(final JsonEnvelope jsonEnvelope, final String source, final String component) {
        final String eventName = jsonEnvelope.metadata().name();

        if (subscriptionEventNamesProvider.accepts(eventName, source, component)) {
            validateAgainstSchema(jsonEnvelope, eventName);
        }
    }

    private void validateAgainstSchema(final JsonEnvelope jsonEnvelope, final String eventName) {
        try {
            final MediaType mediaType = nameToMediaTypeConverter.convert(eventName);
            final String envelopeJson = jsonObjectEnvelopeConverter.asJsonString(jsonEnvelope);
            jsonSchemaValidator.validate(envelopeJson, eventName, of(mediaType));
        } catch (final JsonSchemaValidationException jsonSchemaValidationException) {
            logger.debug(format("JSON schema validation has failed for %s due to %s",
                    eventName,
                    jsonValidationLoggerHelper.toValidationTrace(jsonSchemaValidationException)));
            throw jsonSchemaValidationException;
        }
    }
}
