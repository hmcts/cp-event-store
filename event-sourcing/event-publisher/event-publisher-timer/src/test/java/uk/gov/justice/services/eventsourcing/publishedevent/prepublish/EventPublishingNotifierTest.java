package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.publishedevent.publish.PublishedEventDeQueuerAndPublisher;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.EventPublishingNotifier;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;

import javax.enterprise.concurrent.ManagedExecutorService;

@ExtendWith(MockitoExtension.class)
class EventPublishingNotifierTest {

    @Mock
    private PublishedEventDeQueuerAndPublisher publishedEventDeQueuerAndPublisher;

    @Mock
    private PublisherTimerConfig publisherTimerConfig;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @InjectMocks
    private EventPublishingNotifier eventPublishingNotifier;

    @Test
    void shouldWakeUpWhenRequested() {
        eventPublishingNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        // First wakeUp starts it
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

        // Second wakeUp should not submit again
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService, times(1)).submit(any(Runnable.class));
    }

    @Test
    void shouldProcessEventsWhenRunning() {
        when(publisherTimerConfig.getBackoffMinMilliseconds()).thenReturn(10L);
        when(publisherTimerConfig.getBackoffMultiplier()).thenReturn(1.5);
        when(publisherTimerConfig.getBackoffMaxMilliseconds()).thenReturn(100L);

        when(publishedEventDeQueuerAndPublisher.deQueueAndPublish())
                .thenReturn(true)
                .thenReturn(false)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        eventPublishingNotifier.wakeUp(true);

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        Runnable runnable = captor.getValue();

        runnable.run();

        verify(publishedEventDeQueuerAndPublisher, times(3)).deQueueAndPublish();
    }
}
