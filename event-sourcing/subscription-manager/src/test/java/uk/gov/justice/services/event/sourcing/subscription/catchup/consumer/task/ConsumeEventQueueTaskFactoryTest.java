package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;

import java.util.Queue;
import java.util.UUID;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class ConsumeEventQueueTaskFactoryTest {

    @Inject
    private ConsumeEventQueueBean consumeEventQueueBean;

    @InjectMocks
    private ConsumeEventQueueTaskFactory consumeEventQueueTaskFactory;

    @Test
    public void shouldCreateConsumeEventQueueTask() throws Exception {

        final Queue<LinkedEvent> events = mock(Queue.class);
        final String subscriptionName = "subscription name";
        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final UUID commandId = randomUUID();

        final ConsumeEventQueueTask consumeEventQueueTask = consumeEventQueueTaskFactory.createConsumeEventQueueTask(
                events,
                subscriptionName,
                catchupCommand,
                commandId
        );

        assertThat(getValueOfField(consumeEventQueueTask, "consumeEventQueueBean", ConsumeEventQueueBean.class), is(consumeEventQueueBean));
        assertThat(getValueOfField(consumeEventQueueTask, "events", Queue.class), is(events));
        assertThat(getValueOfField(consumeEventQueueTask, "subscriptionName", String.class), is(subscriptionName));
        assertThat(getValueOfField(consumeEventQueueTask, "catchupCommand", CatchupCommand.class), is(catchupCommand));
        assertThat(getValueOfField(consumeEventQueueTask, "commandId", UUID.class), is(commandId));
    }
}
