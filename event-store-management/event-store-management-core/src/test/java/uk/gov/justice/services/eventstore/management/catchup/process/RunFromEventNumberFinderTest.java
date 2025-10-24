package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class RunFromEventNumberFinderTest {

    @Mock
    private LinkedEventSource linkedEventSource;

    @Mock
    private Logger logger;

    @InjectMocks
    private RunFromEventNumberFinder runFromEventNumberFinder;

    @Test
    public void shouldGetEventNumberForEvent() throws Exception {

        final UUID eventId = randomUUID();
        final String source = "some-context-name";
        final Long eventNumber = 9827394873L;

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(linkedEventSource.findByEventId(eventId)).thenReturn(of(linkedEvent));
        when(linkedEvent.getEventNumber()).thenReturn(of(eventNumber));

        assertThat(runFromEventNumberFinder.findEventNumberToRunFrom(of(eventId), source), is(eventNumber));
    }

    @Test
    public void shouldReturnDefaultFirstEventNumberIfNoRunFromEventNumberProvided() throws Exception {

        final String source = "some-context-name";
        final Optional<UUID> emptyRunFromEventNumber = empty();

        assertThat(runFromEventNumberFinder.findEventNumberToRunFrom(emptyRunFromEventNumber, source), is(1L));
    }

    @Test
    public void shouldReturnDefaultFirstEventNumberIfEventNotFoundInEventStore() throws Exception {

        final UUID eventId = fromString("370256f0-4c89-45e8-81ca-43ced6a8c3d6");
        final String source = "some-context-name";
        final Long defaultFirstEventNumber = 1L;

        when(linkedEventSource.findByEventId(eventId)).thenReturn(empty());

        assertThat(runFromEventNumberFinder.findEventNumberToRunFrom(of(eventId), source), is(defaultFirstEventNumber));

        verify(logger).info("No event with id '370256f0-4c89-45e8-81ca-43ced6a8c3d6' found in 'some-context-name' event_log table. Running from eventNumber '1'");
    }

    @Test
    public void shouldThrowMissingEventNumberExceptionIfEventFoundInEventLogTableButEventNumberIsNull() throws Exception {

        final UUID eventId = fromString("138e292c-6412-404f-b5ce-e599650e51c5");
        final String source = "some-context-name";
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(linkedEventSource.findByEventId(eventId)).thenReturn(of(linkedEvent));
        when(linkedEvent.getEventNumber()).thenReturn(empty());

        final MissingEventNumberException missingEventNumberException = assertThrows(
                MissingEventNumberException.class,
                () -> runFromEventNumberFinder.findEventNumberToRunFrom(of(eventId), source));

        assertThat(missingEventNumberException.getMessage(), is("Event with id '138e292c-6412-404f-b5ce-e599650e51c5' found in 'some-context-name' event_log table has null event number"));
    }
}