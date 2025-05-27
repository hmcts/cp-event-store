package uk.gov.justice.services.event.sourcing.subscription.catchup;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.openejb.jee.WebApp;
import org.apache.openejb.junit5.RunWithApplicationComposer;
import org.apache.openejb.testing.Application;
import org.apache.openejb.testing.Classes;
import org.apache.openejb.testing.Configuration;
import org.apache.openejb.testing.Module;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.cdi.LoggerProducer;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.DummyEventQueueProcessingConfig;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.ConcurrentEventStreamConsumerManager;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventStreamsInProgressList;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventsInProcessCounterProvider;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.ConsumeEventQueueBean;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.ConsumeEventQueueTaskFactory;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.ConsumeEventQueueTaskManager;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.EventProcessingFailedHandler;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.EventQueueConsumer;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.util.DummyCatchupEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.util.TestCatchupBean;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.test.utils.core.messaging.Poller;

import javax.inject.Inject;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.core.postgres.OpenEjbConfigurationBuilder.createOpenEjbConfigurationBuilder;

@RunWithApplicationComposer
public class EventStreamCatchupIT {

    @Inject
    TestCatchupBean testCatchupBean;

    @Inject
    DummyCatchupEventProcessor dummyTransactionalEventProcessor;

    private final Poller poller = new Poller(60, 1000L);

    @Module
    @Classes(cdi = true, value = {
            TestCatchupBean.class,
            DummyCatchupEventProcessor.class,
            EventStreamsInProgressList.class,
            ConsumeEventQueueBean.class,
            LoggerProducer.class,
            DummySystemCommandStore.class,
            ConcurrentEventStreamConsumerManager.class,
            EventProcessingFailedHandler.class,
            ConsumeEventQueueTaskManager.class,
            ConsumeEventQueueTaskFactory.class,
            EventsInProcessCounterProvider.class,
            DummyEventQueueProcessingConfig.class,
            EventQueueConsumer.class
    })
    public WebApp war() {
        return new WebApp()
                .contextRoot("ApplicationComposer")
                .addServlet("App", Application.class.getName());
    }

    @Configuration
    public Properties configuration() {
        return createOpenEjbConfigurationBuilder()
                .addInitialContext()
                .addPostgresqlEventStore()
                .build();
    }

    @Test
    public void shouldRunTheMultiThreadedCatchupWithManyEvents() throws Exception {

        final StopWatch stopWatch = new StopWatch();
        testCatchupBean.run(stopWatch);

        final Optional<Queue<PublishedEvent>> events = poller.pollUntilFound(() -> {
            if (dummyTransactionalEventProcessor.isComplete()) {
                return of(dummyTransactionalEventProcessor.getPublishedEvents());
            }

            return empty();
        });

        stopWatch.stop();

        if (events.isPresent()) {
            System.out.println("Received " + events.get().size() + " in total in " + stopWatch.getTime() + " milliseconds");
        } else {
            fail();
        }
    }
}
