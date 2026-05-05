package uk.gov.justice.services.eventstore.management.catchup.observers;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.CommandResult;
import uk.gov.justice.services.eventstore.management.catchup.state.CatchupError;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;


@ExtendWith(MockitoExtension.class)
public class CatchupCompletionEventFirerTest {

    @Mock
    private Event<SystemCommandStateChangedEvent> systemCommandStateChangedEventFirer;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @InjectMocks
    private CatchupCompletionEventFirer catchupCompletionEventFirer;

    @Captor
    private ArgumentCaptor<SystemCommandStateChangedEvent> systemCommandStateChangedEventCaptor;

    @Test
    public void shouldCompleteSuccessfully() throws Exception {
        final UUID commandId = randomUUID();
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final ZonedDateTime completedAt = ZonedDateTime.of(2016, 10, 10, 23, 23, 23, 0, UTC);

        when(clock.now()).thenReturn(completedAt);

        catchupCompletionEventFirer.completeSuccessfully(commandId, catchupCommand);

        verify(systemCommandStateChangedEventFirer).fire(systemCommandStateChangedEventCaptor.capture());

        final SystemCommandStateChangedEvent stateChangedEvent = systemCommandStateChangedEventCaptor.getValue();

        assertThat(stateChangedEvent.getCommandId(), is(commandId));
        assertThat(stateChangedEvent.getCommandState(), is(COMMAND_COMPLETE));
        assertThat(stateChangedEvent.getSystemCommand(), is(catchupCommand));
        assertThat(stateChangedEvent.getStatusChangedAt(), is(completedAt));
        assertThat(stateChangedEvent.getMessage(), is("CATCHUP successfully completed with 0 errors at 2016-10-10T23:23:23Z"));

        verify(logger).info("CATCHUP successfully completed with 0 errors at 2016-10-10T23:23:23Z");
    }

    @Test
    public void shouldFailCatchup() throws Exception {
        final UUID commandId = randomUUID();
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final ZonedDateTime completedAt = ZonedDateTime.of(2016, 10, 10, 23, 23, 23, 0, UTC);

        final List<CatchupError> errors = asList(mock(CatchupError.class), mock(CatchupError.class));
        when(clock.now()).thenReturn(completedAt);

        catchupCompletionEventFirer.failCatchup(commandId, catchupCommand, errors);

        verify(systemCommandStateChangedEventFirer).fire(systemCommandStateChangedEventCaptor.capture());

        final SystemCommandStateChangedEvent stateChangedEvent = systemCommandStateChangedEventCaptor.getValue();

        assertThat(stateChangedEvent.getCommandId(), is(commandId));
        assertThat(stateChangedEvent.getCommandState(), is(COMMAND_FAILED));
        assertThat(stateChangedEvent.getSystemCommand(), is(catchupCommand));
        assertThat(stateChangedEvent.getStatusChangedAt(), is(completedAt));
        assertThat(stateChangedEvent.getMessage(), is("CATCHUP failed with 2 errors at 2016-10-10T23:23:23Z"));

        verify(logger).error("CATCHUP failed with 2 errors at 2016-10-10T23:23:23Z");
    }

    @Test
    public void shouldFailVerification() throws Exception {

        final UUID commandId = randomUUID();
        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final String errorMessage = "verification failed with 23 errors";

        final CommandResult commandResult = mock(CommandResult.class);

        final ZonedDateTime completedAt = ZonedDateTime.of(2016, 10, 10, 23, 23, 23, 0, UTC);

        when(clock.now()).thenReturn(completedAt);
        when(commandResult.getMessage()).thenReturn(errorMessage);

        catchupCompletionEventFirer.failVerification(commandId, catchupCommand, commandResult);

        verify(systemCommandStateChangedEventFirer).fire(systemCommandStateChangedEventCaptor.capture());

        final SystemCommandStateChangedEvent stateChangedEvent = systemCommandStateChangedEventCaptor.getValue();

        assertThat(stateChangedEvent.getCommandId(), is(commandId));
        assertThat(stateChangedEvent.getCommandState(), is(COMMAND_FAILED));
        assertThat(stateChangedEvent.getSystemCommand(), is(catchupCommand));
        assertThat(stateChangedEvent.getStatusChangedAt(), is(completedAt));
        assertThat(stateChangedEvent.getMessage(), is("CATCHUP run successfully but failed verification: verification failed with 23 errors"));

        verify(logger).error("CATCHUP run successfully but failed verification: verification failed with 23 errors");
    }
}
