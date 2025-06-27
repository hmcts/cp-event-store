package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class StreamErrorConverter {

    @Inject
    private ExceptionHashGenerator exceptionHashGenerator;

    @Inject
    private UtcClock clock;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    public StreamError asStreamError(
            final ExceptionDetails exceptionDetails,
            final JsonEnvelope event,
            final String componentName) {

        final UUID id = randomUUID();
        final StackTraceElement stackTraceElement = exceptionDetails.stackTraceElements().get(0);
        final Optional<Throwable> cause = exceptionDetails.cause();
        final String exceptionClassName = exceptionDetails.originalException().getClass().getName();
        final String exceptionMessage = exceptionDetails.originalException().getMessage();
        final Optional<String> causeClassName = cause.map(causeException -> causeException.getClass().getName());
        final Optional<String> causeMessage = cause.map(Throwable::getMessage);

        final String hash = exceptionHashGenerator.createHashStringFrom(
                stackTraceElement,
                exceptionClassName,
                causeClassName);

        final String javaClassname = stackTraceElement.getClassName();
        final String javaMethod = stackTraceElement.getMethodName();
        final int javaLineNumber = stackTraceElement.getLineNumber();
        final String eventName = event.metadata().name();
        final UUID eventId = event.metadata().id();
        final UUID streamId = event.metadata().streamId()
                .orElseThrow(() -> new MissingStreamIdException(format("No stream id found in event JsonEnvelope. Event name: '%s', eventId: '%s'", eventName, eventId)));
        final Long positionInStream = event.metadata().position()
                .orElseThrow(() -> new MissingPositionInStreamException(format("No positionInStream found in event JsonEnvelope. Event name: '%s', eventId: '%s'", eventName, eventId)));
        final String source = eventSourceNameCalculator.getSource(event);
        final ZonedDateTime dateCreated = clock.now();
        final String fullStackTrace = exceptionDetails.fullStackTrace();

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                hash,
                exceptionClassName,
                causeClassName,
                javaClassname,
                javaMethod,
                javaLineNumber
        );

        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                id,
                hash,
                exceptionMessage,
                causeMessage,
                eventName,
                eventId,
                streamId,
                positionInStream,
                dateCreated,
                fullStackTrace,
                componentName,
                source
        );

        return new StreamError(streamErrorDetails, streamErrorHash);
    }
}
