package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.common.log.LoggerConstants.METADATA;
import static uk.gov.justice.services.common.log.LoggerConstants.REQUEST_DATA;
import static uk.gov.justice.services.common.log.LoggerConstants.SERVICE_CONTEXT;

import uk.gov.justice.services.common.configuration.ServiceContextNameProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.logging.TraceLogger;

import java.util.Optional;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.slf4j.Logger;

public class StreamEventLoggerMetadataAdder {

    private static final String SERVICE_COMPONENT = "serviceComponent";

    @Inject
    private Logger logger;

    @Inject
    private ServiceContextNameProvider serviceContextNameProvider;

    @Inject
    private TraceLogger traceLogger;

    @Inject
    private MdcWrapper mdcWrapper;

    public void addRequestDataToMdc(final JsonEnvelope jsonEnvelope, final String componentName) {
        traceLogger.trace(logger, () -> "Adding Request data to MDC");

        final JsonObjectBuilder builder = Json.createObjectBuilder();

        Optional.ofNullable(serviceContextNameProvider.getServiceContextName())
                .ifPresent(value -> builder.add(SERVICE_CONTEXT, value));

        builder.add(SERVICE_COMPONENT, componentName);

        addMetadataToBuilder(jsonEnvelope, builder);

        mdcWrapper.put(REQUEST_DATA, builder.build().toString());

        traceLogger.trace(logger, () -> "Request data added to MDC");
    }

    public void clearMdc() {
        traceLogger.trace(logger, () -> "Clearing MDC");
        mdcWrapper.clear();
    }

    private void addMetadataToBuilder(final JsonEnvelope jsonEnvelope, final JsonObjectBuilder builder) {
        try {
            builder.add(METADATA, jsonEnvelope.metadata().asJsonObject());
        } catch (final Exception e) {
            builder.add(METADATA, "Could not find: _metadata in envelope");
        }
    }
}
