package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerBean;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerFactory;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerTask;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.WorkerActivityTracker;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.List;

import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import javax.enterprise.concurrent.ManagedExecutorService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private TimerService timerService;

    @Mock
    private ManagedExecutorService managedExecutorService;

    @Mock
    private StreamProcessingConfig streamProcessingConfig;

    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Mock
    private EventPullConfiguration eventPullConfiguration;

    @Mock
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Mock
    private StreamProcessingWorkerFactory streamProcessingWorkerFactory;

    @Mock
    private WorkerActivityTracker workerActivityTracker;

    @Captor
    private ArgumentCaptor<TimerConfig> timerConfigCaptor;

    @InjectMocks
    private StreamProcessingCoordinator streamProcessingCoordinator;

    @Test
    public void shouldCreateOneTimerPerSourceComponentPairOnStartup() {
        final long timerStartWait = 7250L;
        final long timerInterval = 100L;

        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component-1");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component-2");
        final List<SourceComponentPair> pairs = asList(pair1, pair2);

        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(pairs);
        when(streamProcessingConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWait);
        when(streamProcessingConfig.getTimerIntervalMilliseconds()).thenReturn(timerInterval);

        streamProcessingCoordinator.startTimerService();

        verify(timerService, times(2)).createIntervalTimer(
                eq(timerStartWait),
                eq(timerInterval),
                timerConfigCaptor.capture()
        );

        final List<TimerConfig> configs = timerConfigCaptor.getAllValues();
        final SourceComponentPair capturedPair1 = (SourceComponentPair) configs.get(0).getInfo();
        final SourceComponentPair capturedPair2 = (SourceComponentPair) configs.get(1).getInfo();

        org.hamcrest.MatcherAssert.assertThat(capturedPair1, org.hamcrest.CoreMatchers.is(pair1));
        org.hamcrest.MatcherAssert.assertThat(capturedPair2, org.hamcrest.CoreMatchers.is(pair2));
    }

    @Test
    public void shouldNotCreateTimersWhenPullMechanismIsOff() {
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(false);

        streamProcessingCoordinator.startTimerService();

        verifyNoInteractions(subscriptionSourceComponentFinder, timerService);
    }

    @Test
    public void shouldSpawnMaxWorkersWhenRecentlyActive() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);
        final StreamProcessingWorkerTask task = mock(StreamProcessingWorkerTask.class);

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenReturn(true);
        when(workerActivityTracker.getActiveCount(pair)).thenReturn(2);
        when(streamProcessingConfig.getMaxWorkers()).thenReturn(15);
        when(streamProcessingWorkerFactory.createWorkerTask(pair)).thenReturn(task);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(managedExecutorService, times(13)).execute(task);
    }

    @Test
    public void shouldNotSpawnWorkersWhenRecentlyActiveButAtMaxWorkers() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenReturn(true);
        when(workerActivityTracker.getActiveCount(pair)).thenReturn(15);
        when(streamProcessingConfig.getMaxWorkers()).thenReturn(15);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(managedExecutorService, never()).execute(any(Runnable.class));
    }

    @Test
    public void shouldRunInlineProbeWhenIdleAndMarkActiveWhenEventsFound() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenReturn(false);
        when(workerActivityTracker.getActiveCount(pair)).thenReturn(0);
        when(streamProcessingConfig.getMaxWorkers()).thenReturn(15);
        when(streamProcessingWorkerBean.processUntilIdle("source", "component")).thenReturn(true);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(streamProcessingWorkerBean).processUntilIdle("source", "component");
        verify(workerActivityTracker).markRecentlyActive(pair);
        verify(managedExecutorService, never()).execute(any(Runnable.class));
    }

    @Test
    public void shouldRunInlineProbeWhenIdleAndNotMarkActiveWhenNoEventsFound() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenReturn(false);
        when(workerActivityTracker.getActiveCount(pair)).thenReturn(0);
        when(streamProcessingConfig.getMaxWorkers()).thenReturn(15);
        when(streamProcessingWorkerBean.processUntilIdle("source", "component")).thenReturn(false);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(streamProcessingWorkerBean).processUntilIdle("source", "component");
        verify(workerActivityTracker, never()).markRecentlyActive(pair);
    }

    @Test
    public void shouldDoNothingWhenWorkersAreActiveButNotRecentlyActive() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenReturn(false);
        when(workerActivityTracker.getActiveCount(pair)).thenReturn(5);
        when(streamProcessingConfig.getMaxWorkers()).thenReturn(15);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(managedExecutorService, never()).execute(any(Runnable.class));
    }

    @Test
    public void shouldLogErrorWhenCoordinationFails() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final Timer timer = mock(Timer.class);
        final RuntimeException exception = new RuntimeException("Coordination failed");

        when(timer.getInfo()).thenReturn(pair);
        when(workerActivityTracker.resetRecentlyActive(pair)).thenThrow(exception);

        streamProcessingCoordinator.coordinateWorkers(timer);

        verify(logger).error("Failed to coordinate workers for source: {}, component: {}",
                "source", "component", exception);
    }
}
