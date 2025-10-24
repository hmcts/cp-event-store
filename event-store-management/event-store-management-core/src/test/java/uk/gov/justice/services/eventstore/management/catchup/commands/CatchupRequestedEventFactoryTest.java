package uk.gov.justice.services.eventstore.management.catchup.commands;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupRequestedEvent;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CatchupRequestedEventFactoryTest {

    @Mock
    private UtcClock clock;

    @InjectMocks
    private CatchupRequestedEventFactory catchupRequestedEventFactory;

    @Test
    public void shouldCreateCatchupRequestedEventWithoutRunFromEventIdIfCommandRuntimeIdIsNull() throws Exception {

        final EventCatchupCommand catchupCommand = new EventCatchupCommand();
        final ZonedDateTime now = new UtcClock().now();
        final UUID commandId = randomUUID();

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = mock(JmxCommandRuntimeParameters.class);

        when(jmxCommandRuntimeParameters.getCommandRuntimeId()).thenReturn(null);
        when(clock.now()).thenReturn(now);

        final CatchupRequestedEvent catchupRequestedEvent = catchupRequestedEventFactory.create(
                catchupCommand,
                commandId,
                jmxCommandRuntimeParameters);

        assertThat(catchupRequestedEvent.getCommandId(), is(commandId));
        assertThat(catchupRequestedEvent.getCatchupCommand(), is(catchupCommand));
        assertThat(catchupRequestedEvent.getCatchupRequestedAt(), is(now));
        assertThat(catchupRequestedEvent.getRunFromEventId(), is(empty()));
    }

    @Test
    public void shouldCreateCatchupRequestedEventWithRunFromEventIdIfCommandRuntimeIdExists() throws Exception {

        final EventCatchupCommand catchupCommand = new EventCatchupCommand();
        final ZonedDateTime now = new UtcClock().now();
        final UUID commandId = randomUUID();
        final UUID commandRuntimeId = randomUUID();

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = mock(JmxCommandRuntimeParameters.class);

        when(jmxCommandRuntimeParameters.getCommandRuntimeId()).thenReturn(commandRuntimeId);
        when(clock.now()).thenReturn(now);

        final CatchupRequestedEvent catchupRequestedEvent = catchupRequestedEventFactory.create(
                catchupCommand,
                commandId,
                jmxCommandRuntimeParameters);

        assertThat(catchupRequestedEvent.getCommandId(), is(commandId));
        assertThat(catchupRequestedEvent.getCatchupCommand(), is(catchupCommand));
        assertThat(catchupRequestedEvent.getCatchupRequestedAt(), is(now));
        assertThat(catchupRequestedEvent.getRunFromEventId(), is(of(commandRuntimeId)));
    }
}