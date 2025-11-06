package uk.gov.justice.services.eventstore.management.replay.commands;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.ReplayEventToEventListenerCommand;
import uk.gov.justice.services.eventstore.management.replay.process.ReplayEventToComponentRunner;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters.JmxCommandRuntimeParametersBuilder;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ReplayEventToEventListenerCommandHandlerTest {

    private static final UUID COMMAND_ID = UUID.randomUUID();
    private static final UUID COMMAND_RUNTIME_ID = UUID.randomUUID();

    private static final ReplayEventToEventListenerCommand COMMAND = new ReplayEventToEventListenerCommand();
    private static final ZonedDateTime NOW = ZonedDateTime.now(ZoneOffset.UTC);

    @Mock
    private Event<SystemCommandStateChangedEvent> stateChangedEventFirer;

    @Mock
    private ReplayEventToComponentRunner replayEventToComponentRunner;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @Captor
    private ArgumentCaptor<SystemCommandStateChangedEvent> eventCaptor;

    @InjectMocks
    private ReplayEventToEventListenerCommandHandler replayEventToEventListenerCommandHandler;

    @Test
    public void onSuccessShouldFireInProgressAndCompletedSystemCommands() {
        when(clock.now()).thenReturn(NOW);

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParametersBuilder()
                .withCommandRuntimeId(COMMAND_RUNTIME_ID)
                .build();

        replayEventToEventListenerCommandHandler.replayEventToEventListener(COMMAND, COMMAND_ID, jmxCommandRuntimeParameters);

        verify(replayEventToComponentRunner).run(COMMAND_ID, COMMAND_RUNTIME_ID, EVENT_LISTENER);

        verify(stateChangedEventFirer, times(2)).fire(eventCaptor.capture());
        final List<SystemCommandStateChangedEvent> actualEvents = eventCaptor.getAllValues();
        final SystemCommandStateChangedEvent inProgressEvent = actualEvents.get(0);

        assertThat(inProgressEvent.getCommandId(), is(COMMAND_ID));
        assertThat(inProgressEvent.getSystemCommand(), is(COMMAND));
        assertThat(inProgressEvent.getCommandState(), is(COMMAND_IN_PROGRESS));
        assertThat(inProgressEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command received"));
        assertThat(inProgressEvent.getStatusChangedAt(), is(NOW));

        final SystemCommandStateChangedEvent completedEvent = actualEvents.get(1);
        assertThat(completedEvent.getCommandId(), is(COMMAND_ID));
        assertThat(completedEvent.getSystemCommand(), is(COMMAND));
        assertThat(completedEvent.getCommandState(), is(COMMAND_COMPLETE));
        assertThat(completedEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command completed"));
        assertThat(completedEvent.getStatusChangedAt(), is(NOW));
    }

    @Test
    public void shouldHandleOptionalEventSourceNameFromCommandRuntimeString() {
        when(clock.now()).thenReturn(NOW);

        final String componentName = "SOME_EVENT_LISTENER";
        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParametersBuilder()
                .withCommandRuntimeId(COMMAND_RUNTIME_ID)
                .withCommandRuntimeString(componentName)
                .build();

        replayEventToEventListenerCommandHandler.replayEventToEventListener(COMMAND, COMMAND_ID, jmxCommandRuntimeParameters);

        verify(replayEventToComponentRunner).run(COMMAND_ID, COMMAND_RUNTIME_ID, componentName);

        verify(stateChangedEventFirer, times(2)).fire(eventCaptor.capture());
        final List<SystemCommandStateChangedEvent> actualEvents = eventCaptor.getAllValues();
        final SystemCommandStateChangedEvent inProgressEvent = actualEvents.get(0);

        assertThat(inProgressEvent.getCommandId(), is(COMMAND_ID));
        assertThat(inProgressEvent.getSystemCommand(), is(COMMAND));
        assertThat(inProgressEvent.getCommandState(), is(COMMAND_IN_PROGRESS));
        assertThat(inProgressEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command received"));
        assertThat(inProgressEvent.getStatusChangedAt(), is(NOW));

        final SystemCommandStateChangedEvent completedEvent = actualEvents.get(1);
        assertThat(completedEvent.getCommandId(), is(COMMAND_ID));
        assertThat(completedEvent.getSystemCommand(), is(COMMAND));
        assertThat(completedEvent.getCommandState(), is(COMMAND_COMPLETE));
        assertThat(completedEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command completed"));
        assertThat(completedEvent.getStatusChangedAt(), is(NOW));
    }

    @Test
    public void onSuccessShouldFireInProgressAndFailedyStemCommands() {
        final RuntimeException exception = new RuntimeException();
        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParametersBuilder()
                .withCommandRuntimeId(COMMAND_RUNTIME_ID)
                .build();
        when(clock.now()).thenReturn(NOW);
        doThrow(exception).when(replayEventToComponentRunner).run(any(), any(), any());

        replayEventToEventListenerCommandHandler.replayEventToEventListener(COMMAND, COMMAND_ID, jmxCommandRuntimeParameters);

        verify(stateChangedEventFirer, times(2)).fire(eventCaptor.capture());
        final List<SystemCommandStateChangedEvent> actualEvents = eventCaptor.getAllValues();
        final SystemCommandStateChangedEvent inProgressEvent = actualEvents.get(0);

        assertThat(inProgressEvent.getCommandId(), is(COMMAND_ID));
        assertThat(inProgressEvent.getSystemCommand(), is(COMMAND));
        assertThat(inProgressEvent.getCommandState(), is(COMMAND_IN_PROGRESS));
        assertThat(inProgressEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command received"));
        assertThat(inProgressEvent.getStatusChangedAt(), is(NOW));

        final SystemCommandStateChangedEvent completedEvent = actualEvents.get(1);
        assertThat(completedEvent.getCommandId(), is(COMMAND_ID));
        assertThat(completedEvent.getSystemCommand(), is(COMMAND));
        assertThat(completedEvent.getCommandState(), is(COMMAND_FAILED));
        assertThat(completedEvent.getMessage(), is("REPLAY_EVENT_TO_EVENT_LISTENER command failed"));

        verify(logger).error("REPLAY_EVENT_TO_EVENT_LISTENER failed. commandId {}, commandRuntimeId {}", COMMAND_ID, COMMAND_RUNTIME_ID, exception);
    }
}