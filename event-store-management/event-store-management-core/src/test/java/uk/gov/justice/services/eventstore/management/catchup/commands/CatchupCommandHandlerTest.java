package uk.gov.justice.services.eventstore.management.catchup.commands;

import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.of;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters.withNoCommandParameters;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.IndexerCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupRequestedEvent;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class CatchupCommandHandlerTest {

    @Mock
    private Event<CatchupRequestedEvent> catchupRequestedEventFirer;

    @Mock
    private CatchupRequestedEventFactory catchupRequestedEventFactory;

    @Mock
    private Logger logger;

    @InjectMocks
    private CatchupCommandHandler catchupCommandHandler;

    @Test
    public void shouldFireEventCatchup() throws Exception {

        final UUID commandId = randomUUID();
        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();
        final ZonedDateTime now = of(2019, 8, 23, 11, 22, 1, 0, UTC);

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = mock(JmxCommandRuntimeParameters.class);
        final CatchupRequestedEvent catchupRequestedEvent = mock(CatchupRequestedEvent.class);

        when(catchupRequestedEventFactory.create(eventCatchupCommand, commandId, jmxCommandRuntimeParameters)).thenReturn(catchupRequestedEvent);
        when(catchupRequestedEvent.getCatchupRequestedAt()).thenReturn(now);

        catchupCommandHandler.catchupEvents(eventCatchupCommand, commandId, jmxCommandRuntimeParameters);

        verify(logger).info("Received command 'CATCHUP' at 11:22:01 AM");
        verify(catchupRequestedEventFirer).fire(catchupRequestedEvent);
    }

    @Test
    public void shouldFireIndexCatchup() throws Exception {

        final UUID commandId = randomUUID();
        final IndexerCatchupCommand indexerCatchupCommand = new IndexerCatchupCommand();
        final ZonedDateTime now = of(2019, 8, 23, 11, 22, 1, 0, UTC);

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = mock(JmxCommandRuntimeParameters.class);
        final CatchupRequestedEvent catchupRequestedEvent = mock(CatchupRequestedEvent.class);

        when(catchupRequestedEventFactory.create(indexerCatchupCommand, commandId, jmxCommandRuntimeParameters)).thenReturn(catchupRequestedEvent);
        when(catchupRequestedEvent.getCatchupRequestedAt()).thenReturn(now);

        catchupCommandHandler.catchupSearchIndexes(indexerCatchupCommand, commandId, jmxCommandRuntimeParameters);

        verify(logger).info("Received command 'INDEXER_CATCHUP' at 11:22:01 AM");
        verify(catchupRequestedEventFirer).fire(catchupRequestedEvent);
    }
}
