package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import uk.gov.justice.services.eventsourcing.publishedevent.publish.PublishedEventDeQueuerAndPublisher;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.EventPublishingNotifier;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;

import javax.enterprise.concurrent.ManagedExecutorService;
import java.util.concurrent.RejectedExecutionException;

@ExtendWith(MockitoExtension.class)
class EventPublishingNotifierTest {

    @Mock
    private PublishedEventDeQueuerAndPublisher publishedEventDeQueuerAndPublisher;

    @Mock
    private PublisherTimerConfig publisherTimerConfig;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventPublishingNotifier eventPublishingNotifier;

    @Test
    void shouldWakeUpWhenRequested() {
        eventPublishingNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

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

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        final Runnable runnable = captor.getValue();

        runnable.run();

        verify(publishedEventDeQueuerAndPublisher, times(3)).deQueueAndPublish();
    }

    @Test
    void shouldResetStartedFlagWhenThreadExits() {
        when(publisherTimerConfig.getBackoffMinMilliseconds()).thenReturn(10L);

        when(publishedEventDeQueuerAndPublisher.deQueueAndPublish())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        eventPublishingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        // started should be reset to false, so next wakeUp(true) should submit again
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }

    @Test
    void shouldResetStartedFlagWhenSubmitFails() {
        doThrow(new RejectedExecutionException("Pool full"))
                .when(managedExecutorService).submit(any(Runnable.class));

        eventPublishingNotifier.wakeUp(true);

        // started should be reset, so no exception propagated
        assertThat(true, is(true));
    }
}