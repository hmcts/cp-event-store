package uk.gov.justice.services.eventsourcing.eventpublishing;

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

import java.util.concurrent.RejectedExecutionException;

import jakarta.enterprise.concurrent.ManagedExecutorService;

@ExtendWith(MockitoExtension.class)
class EventPublishingNotifierTest {

    @Mock
    private LinkedEventPublisher linkedEventPublisher;

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
        when(linkedEventPublisher.publishNextNewEvent())
                .thenReturn(true)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        eventPublishingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        verify(linkedEventPublisher, times(2)).publishNextNewEvent();
    }

    @Test
    void shouldResetStartedFlagWhenThreadExits() {
        when(linkedEventPublisher.publishNextNewEvent())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        eventPublishingNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }

    @Test
    void shouldResetStartedFlagWhenSubmitFails() {
        doThrow(new RejectedExecutionException("Pool full"))
                .when(managedExecutorService).submit(any(Runnable.class));

        eventPublishingNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
        eventPublishingNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }
}