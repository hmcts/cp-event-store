package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorConverterTest {

    @Mock
    private ExceptionHashGenerator exceptionHashGenerator;

    @Mock
    private UtcClock clock;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @InjectMocks
    private StreamErrorConverter streamErrorConverter;

    @Test
    public void shouldConvertExceptionDetailsAndEventIntoEventError() throws Exception {

        final NullPointerException causeException = new NullPointerException("Ooops");
        final RuntimeException exception = new RuntimeException("Something went wrogn", causeException);
        final String componentName = "SOME_COMPONENT";
        final String source = "usersgroups";

        final String fullStackTrace = "Full stack trace";
        final String javaClassName = "uk.gov.justice.eventbuffer.core.error.SomeFailingJavaClass";
        final String methodName = "someJavaMethod";
        final int lineNumber = 234;
        final Long positionInStream = 12345L;

        final String hash = "kshdkfhkjsdfhkjsdhfkj";
        final ZonedDateTime dateCreated = new UtcClock().now();
        final String eventName = "context.events.something.happened";
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();

        final StackTraceElement earliestStackTraceElement = mock(StackTraceElement.class);
        final StackTraceElement laterStackTraceElement = mock(StackTraceElement.class);
        final JsonEnvelope event = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        final List<StackTraceElement> stackTraceElements = List.of(earliestStackTraceElement, laterStackTraceElement);
        final ExceptionDetails exceptionDetails = new ExceptionDetails(
                exception,
                of(causeException),
                stackTraceElements,
                fullStackTrace
        );

        when(clock.now()).thenReturn(dateCreated);
        when(exceptionHashGenerator.createHashStringFrom(
                earliestStackTraceElement,
                exception.getClass().getName(),
                of(causeException.getClass().getName()))).thenReturn(hash);
        when(event.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.position()).thenReturn(of(positionInStream));
        when(eventSourceNameCalculator.getSource(event)).thenReturn(source);

        when(earliestStackTraceElement.getClassName()).thenReturn(javaClassName);
        when(earliestStackTraceElement.getMethodName()).thenReturn(methodName);
        when(earliestStackTraceElement.getLineNumber()).thenReturn(lineNumber);

        final StreamError streamError = streamErrorConverter.asStreamError(exceptionDetails, event, componentName);
        final StreamErrorOccurrence streamErrorOccurrence = streamError.streamErrorOccurrence();

        assertThat(streamErrorOccurrence.id(), is(instanceOf(UUID.class)));
        assertThat(streamErrorOccurrence.eventName(), is(eventName));
        assertThat(streamErrorOccurrence.eventId(), is(eventId));
        assertThat(streamErrorOccurrence.streamId(), is(streamId));
        assertThat(streamErrorOccurrence.positionInStream(), is(positionInStream));
        assertThat(streamErrorOccurrence.dateCreated(), is(dateCreated));
        assertThat(streamErrorOccurrence.exceptionMessage(), is("Something went wrogn"));
        assertThat(streamErrorOccurrence.causeMessage(), is(of("Ooops")));
        assertThat(streamErrorOccurrence.fullStackTrace(), is(fullStackTrace));
        assertThat(streamErrorOccurrence.componentName(), is(componentName));
        assertThat(streamErrorOccurrence.source(), is(source));

        final StreamErrorHash streamErrorHash = streamError.streamErrorHash();

        assertThat(streamErrorHash.hash(), is(hash));
        assertThat(streamErrorHash.exceptionClassName(), is(exception.getClass().getName()));
        assertThat(streamErrorHash.causeClassName(), is(of(causeException.getClass().getName())));
        assertThat(streamErrorHash.javaClassName(), is(javaClassName));
        assertThat(streamErrorHash.javaMethod(), is(methodName));
        assertThat(streamErrorHash.javaLineNumber(), is(lineNumber));
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfStreamIdIsEmptyInJsonEnvelope() throws Exception {

        final NullPointerException causeException = new NullPointerException("Ooops");
        final RuntimeException exception = new RuntimeException("Something went wrogn", causeException);

        final String componentName = "SOME_COMPONENT";
        final String fullStackTrace = "Full stack trace";
        final String javaClassName = "uk.gov.justice.eventbuffer.core.error.SomeFailingJavaClass";
        final String methodName = "someJavaMethod";
        final int lineNumber = 234;

        final String hash = "kshdkfhkjsdfhkjsdhfkj";
        final String eventName = "context.events.something.happened";
        final UUID eventId = fromString("74a2b139-cba7-4430-b200-b322c3729b1f");

        final StackTraceElement earliestStackTraceElement = mock(StackTraceElement.class);
        final StackTraceElement laterStackTraceElement = mock(StackTraceElement.class);
        final JsonEnvelope event = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);


        final List<StackTraceElement> stackTraceElements = List.of(earliestStackTraceElement, laterStackTraceElement);
        final ExceptionDetails exceptionDetails = new ExceptionDetails(
                exception,
                of(causeException),
                stackTraceElements,
                fullStackTrace
        );

        when(exceptionHashGenerator.createHashStringFrom(
                earliestStackTraceElement,
                exception.getClass().getName(),
                of(causeException.getClass().getName()))).thenReturn(hash);
        when(event.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(empty());

        when(earliestStackTraceElement.getClassName()).thenReturn(javaClassName);
        when(earliestStackTraceElement.getMethodName()).thenReturn(methodName);
        when(earliestStackTraceElement.getLineNumber()).thenReturn(lineNumber);

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> streamErrorConverter.asStreamError(exceptionDetails, event, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No stream id found in event JsonEnvelope. Event name: 'context.events.something.happened', eventId: '74a2b139-cba7-4430-b200-b322c3729b1f'"));
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfPositionIsEmptyInJsonEnvelope() throws Exception {

        final NullPointerException causeException = new NullPointerException("Ooops");
        final RuntimeException exception = new RuntimeException("Something went wrogn", causeException);

        final String componentName = "SOME_COMPONENT";
        final String fullStackTrace = "Full stack trace";
        final String javaClassName = "uk.gov.justice.eventbuffer.core.error.SomeFailingJavaClass";
        final String methodName = "someJavaMethod";
        final int lineNumber = 234;

        final String hash = "kshdkfhkjsdfhkjsdhfkj";
        final String eventName = "context.events.something.happened";
        final UUID eventId = fromString("7de6f031-8d1e-4e3b-9f03-112a9af80690");
        final UUID streamId = randomUUID();

        final StackTraceElement earliestStackTraceElement = mock(StackTraceElement.class);
        final StackTraceElement laterStackTraceElement = mock(StackTraceElement.class);
        final JsonEnvelope event = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        final List<StackTraceElement> stackTraceElements = List.of(earliestStackTraceElement, laterStackTraceElement);
        final ExceptionDetails exceptionDetails = new ExceptionDetails(
                exception,
                of(causeException),
                stackTraceElements,
                fullStackTrace
        );

        when(exceptionHashGenerator.createHashStringFrom(
                earliestStackTraceElement,
                exception.getClass().getName(),
                of(causeException.getClass().getName()))).thenReturn(hash);
        when(event.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.position()).thenReturn(empty());

        when(earliestStackTraceElement.getClassName()).thenReturn(javaClassName);
        when(earliestStackTraceElement.getMethodName()).thenReturn(methodName);
        when(earliestStackTraceElement.getLineNumber()).thenReturn(lineNumber);

        final MissingPositionInStreamException missingPoisitionInStreamException = assertThrows(
                MissingPositionInStreamException.class,
                () -> streamErrorConverter.asStreamError(exceptionDetails, event, componentName));

        assertThat(missingPoisitionInStreamException.getMessage(), is("No positionInStream found in event JsonEnvelope. Event name: 'context.events.something.happened', eventId: '7de6f031-8d1e-4e3b-9f03-112a9af80690'"));
    }
}