package uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands;

import static java.util.UUID.fromString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class RebuildSnapshotCommandCommandHandlerTest {

  @Mock
  private Event<SystemCommandStateChangedEvent> stateChangedEventFirer;

  @Mock
  private RegenerateAggregateSnapshotBean regenerateAggregateSnapshotBean;

  @Mock
  private UtcClock clock;

  @Mock
  private Logger logger;

  @InjectMocks
  private RebuildSnapshotCommandCommandHandler rebuildSnapshotCommandCommandHandler;

  @Test
  public void shouldHandleRebuildSnapshotsJmxCommand() throws Exception {

    final ZonedDateTime commandReceivedTime = new UtcClock().now();
    final ZonedDateTime commandCompleteTime = commandReceivedTime.plusSeconds(1);
    final UUID commandId = fromString("d68d82d2-c532-4fd5-9b6e-0d7c481c44bf");
    final UUID streamId = fromString("00327114-174d-4061-b60d-d5af3ec6b17d");
    final String aggregateClassName = "some-aggregate-class";
    final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParameters.JmxCommandRuntimeParametersBuilder()
            .withCommandRuntimeId(streamId)
            .withCommandRuntimeString(aggregateClassName)
            .build();

    final RebuildSnapshotCommand rebuildSnapshotCommand = new RebuildSnapshotCommand();

    when(clock.now()).thenReturn(commandReceivedTime, commandCompleteTime);

    rebuildSnapshotCommandCommandHandler.regenerateAggregateSnapshot(
            rebuildSnapshotCommand,
            commandId,
            jmxCommandRuntimeParameters);

    final InOrder inOrder = inOrder(stateChangedEventFirer, regenerateAggregateSnapshotBean, logger);
    inOrder.verify(stateChangedEventFirer).fire(new SystemCommandStateChangedEvent(
            commandId,
            rebuildSnapshotCommand,
            COMMAND_IN_PROGRESS,
            commandReceivedTime,
            "REBUILD_SNAPSHOTS command received for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'"
    ));
    inOrder.verify(logger).info("REBUILD_SNAPSHOTS command received for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'");
    inOrder.verify(regenerateAggregateSnapshotBean).runAggregateSnapshotRegeneration(streamId, aggregateClassName);
    inOrder.verify(stateChangedEventFirer).fire(new SystemCommandStateChangedEvent(
            commandId,
            rebuildSnapshotCommand,
            COMMAND_COMPLETE,
            commandCompleteTime,
            "REBUILD_SNAPSHOTS command completed for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'"
    ));
    inOrder.verify(logger).info("REBUILD_SNAPSHOTS command complete for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'");
  }

  @Test
  public void shouldFireCommandFailedIfGeneratingSnapshotFails() throws Exception {

    final ZonedDateTime commandReceivedTime = new UtcClock().now();
    final ZonedDateTime commandCompleteTime = commandReceivedTime.plusSeconds(1);
    final UUID commandId = fromString("d68d82d2-c532-4fd5-9b6e-0d7c481c44bf");
    final UUID streamId = fromString("00327114-174d-4061-b60d-d5af3ec6b17d");
    final String aggregateClassName = "some-aggregate-class";
    final AggregateSnapshotGenerationFailedException aggregateSnapshotGenerationFailedException = new AggregateSnapshotGenerationFailedException("Ooops");
    final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParameters.JmxCommandRuntimeParametersBuilder()
            .withCommandRuntimeId(streamId)
            .withCommandRuntimeString(aggregateClassName)
            .build();

    final RebuildSnapshotCommand rebuildSnapshotCommand = new RebuildSnapshotCommand();

    when(clock.now()).thenReturn(commandReceivedTime, commandCompleteTime);
    doThrow(aggregateSnapshotGenerationFailedException).when(regenerateAggregateSnapshotBean).runAggregateSnapshotRegeneration(streamId, aggregateClassName);

    rebuildSnapshotCommandCommandHandler.regenerateAggregateSnapshot(
            rebuildSnapshotCommand,
            commandId,
            jmxCommandRuntimeParameters);

    final InOrder inOrder = inOrder(stateChangedEventFirer, regenerateAggregateSnapshotBean, logger);
    inOrder.verify(stateChangedEventFirer).fire(new SystemCommandStateChangedEvent(
            commandId,
            rebuildSnapshotCommand,
            COMMAND_IN_PROGRESS,
            commandReceivedTime,
            "REBUILD_SNAPSHOTS command received for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'"
    ));
    inOrder.verify(logger).info("REBUILD_SNAPSHOTS command received for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'");
    inOrder.verify(regenerateAggregateSnapshotBean).runAggregateSnapshotRegeneration(streamId, aggregateClassName);

    inOrder.verify(stateChangedEventFirer).fire(new SystemCommandStateChangedEvent(
            commandId,
            rebuildSnapshotCommand,
            COMMAND_FAILED,
            commandCompleteTime,
            "REBUILD_SNAPSHOTS command failed for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'"
    ));
    inOrder.verify(logger).error("REBUILD_SNAPSHOTS failed for streamId '00327114-174d-4061-b60d-d5af3ec6b17d' and Aggregate class 'some-aggregate-class'", aggregateSnapshotGenerationFailedException);
  }
}