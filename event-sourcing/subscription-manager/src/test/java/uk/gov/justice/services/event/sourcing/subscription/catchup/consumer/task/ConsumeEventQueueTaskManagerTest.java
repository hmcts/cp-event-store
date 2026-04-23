package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;

import java.util.Queue;
import java.util.UUID;

import jakarta.enterprise.concurrent.ManagedExecutorService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumeEventQueueTaskManagerTest {

    @Mock
    private ManagedExecutorService managedExecutorService;

    @Mock
    private ConsumeEventQueueTaskFactory consumeEventQueueTaskFactory;

    @InjectMocks
    private ConsumeEventQueueTaskManager consumeEventQueueTaskManager;

    @Test
    public void shouldAsynchronouslyRunConsumeEventQueue() throws Exception {

        final Queue<LinkedEvent> events = mock(Queue.class);
        final EventQueueConsumer eventQueueConsumer = mock(EventQueueConsumer.class);
        final String subscriptionName = "subscription name";
        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final UUID commandId = randomUUID();

        final ConsumeEventQueueTask consumeEventQueueTask = mock(ConsumeEventQueueTask.class);

        when(consumeEventQueueTaskFactory.createConsumeEventQueueTask(
                events,
                subscriptionName,
                catchupCommand,
                commandId
        )).thenReturn(consumeEventQueueTask);

        consumeEventQueueTaskManager.consume(
                events,
                subscriptionName,
                catchupCommand,
                commandId
        );

        verify(managedExecutorService).execute(consumeEventQueueTask);
    }
}
