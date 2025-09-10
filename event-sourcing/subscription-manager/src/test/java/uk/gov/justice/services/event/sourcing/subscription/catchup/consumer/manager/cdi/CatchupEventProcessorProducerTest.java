package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.cdi;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.DefaultTransactionalEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.NewSubscriptionAwareEventProcessor;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CatchupEventProcessorProducerTest {

    @Mock
    private EventErrorHandlingConfiguration eventErrorHandlingConfiguration;

    @Mock
    private DefaultTransactionalEventProcessor defaultTransactionalEventProcessor;

    @Mock
    private NewSubscriptionAwareEventProcessor newSubscriptionAwareEventProcessor;

    @InjectMocks
    private CatchupEventProcessorProducer catchupEventProcessorProducer;

    @Test
    void transactionalEventProcessorDefault() {
        PublishedEvent publishedEvent = mock(PublishedEvent.class);
        String compName = "compName";

        when(eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()).thenReturn(false);

        // run
        catchupEventProcessorProducer.transactionalEventProcessor()
                .processWithEventBuffer(publishedEvent, compName);

        // verify
        verify(defaultTransactionalEventProcessor).processWithEventBuffer(eq(publishedEvent), eq(compName));
        verifyNoMoreInteractions(newSubscriptionAwareEventProcessor, defaultTransactionalEventProcessor);
    }

    @Test
    void transactionalEventProcessorNew() {
        PublishedEvent publishedEvent = mock(PublishedEvent.class);
        String compName = "comp2Name";

        when(eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()).thenReturn(true);

        // run
        catchupEventProcessorProducer.transactionalEventProcessor()
                .processWithEventBuffer(publishedEvent, compName);

        // verify
        verify(newSubscriptionAwareEventProcessor).processWithEventBuffer(eq(publishedEvent), eq(compName));
        verifyNoMoreInteractions(newSubscriptionAwareEventProcessor, defaultTransactionalEventProcessor);
    }
}