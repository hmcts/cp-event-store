package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.EventPublishingNotifier;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.enterprise.concurrent.ManagedExecutorService;
import java.util.concurrent.RejectedExecutionException;

@ExtendWith(MockitoExtension.class)
class PrePublishNotifierTest {

    @Mock
    private PrePublishProcessor prePublishProcessor;

    @Mock
    private EventPublishingNotifier eventPublishingNotifier;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @Mock
    private Logger logger;

    @InjectMocks
    private PrePublishNotifier prePublishNotifier;

    @Test
    void shouldWakeUpOnEventAppended() {
        final EventAppendedEvent event = mock(EventAppendedEvent.class);

        prePublishNotifier.onEventAppendedEvent(event);

        verify(managedExecutorService, never()).submit(any(Runnable.class));
    }

    @Test
    void shouldNotSubmitIfAlreadyStarted() {
        prePublishNotifier.wakeUp(true);
        verify(managedExecutorService).submit(any(Runnable.class));

        prePublishNotifier.wakeUp(true);
        verify(managedExecutorService, times(1)).submit(any(Runnable.class));
    }

    @Test
    void shouldProcessEventsWhenRunning() {
        when(prePublishProcessor.prePublishNextEvent())
                .thenReturn(true)
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        prePublishNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        verify(prePublishProcessor, times(2)).prePublishNextEvent();
        verify(eventPublishingNotifier, times(1)).wakeUp(false);
    }

    @Test
    void shouldResetStartedFlagWhenThreadExits() {
        when(prePublishProcessor.prePublishNextEvent())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return false;
                });

        prePublishNotifier.wakeUp(true);

        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(managedExecutorService).submit(captor.capture());
        captor.getValue().run();

        prePublishNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }

    @Test
    void shouldResetStartedFlagWhenSubmitFails() {
        doThrow(new RejectedExecutionException("Pool full"))
                .when(managedExecutorService).submit(any(Runnable.class));

        prePublishNotifier.wakeUp(true);

        verify(managedExecutorService).submit(any(Runnable.class));
        prePublishNotifier.wakeUp(true);
        verify(managedExecutorService, times(2)).submit(any(Runnable.class));
    }
}
