package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.util.List;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamProcessingSubscriptionManager;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingTimerBeanTest {

    @Mock
    private Logger logger;

    @Mock
    private TimerService timerService;

    @Mock
    private StreamProcessingConfig streamProcessingConfig;

    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Mock
    private StreamProcessingSubscriptionManager streamProcessingSubscriptionManager;

    @Mock
    private Timer timer;

    @Mock
    private EventPullConfiguration eventPullConfiguration;

    @Mock
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @Mock
    private SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator;

    @Captor
    private ArgumentCaptor<TimerConfig> timerConfigCaptor;

    @InjectMocks
    private StreamProcessingTimerBean streamProcessingTimerBean;

    @Test
    public void shouldCreateTimerForEachSourceComponentPairAndWorkerOnStartup() {

        final long timerStartWaitMilliseconds = 7250L;
        final long timerIntervalMilliseconds = 1000L;
        final int maxThreads = 3;

        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component-1");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component-2");
        final List<SourceComponentPair> pairs = asList(pair1, pair2);

        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(pairs);
        when(streamProcessingConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(streamProcessingConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);
        when(streamProcessingConfig.getMaxThreads()).thenReturn(maxThreads);

        streamProcessingTimerBean.startTimerService();

        verify(timerService, times(6)).createIntervalTimer(
                eq(timerStartWaitMilliseconds),
                eq(timerIntervalMilliseconds),
                timerConfigCaptor.capture()
        );

        final List<TimerConfig> timerConfigs = timerConfigCaptor.getAllValues();
        assertThat(timerConfigs.size(), is(6));

        final WorkerTimerInfo workerTimerInfo1 = (WorkerTimerInfo) timerConfigs.get(0).getInfo();
        assertThat(workerTimerInfo1.sourceComponentPair(), is(pair1));
        assertThat(workerTimerInfo1.workerNumber(), is(0));

        final WorkerTimerInfo workerTimerInfo2 = (WorkerTimerInfo) timerConfigs.get(1).getInfo();
        assertThat(workerTimerInfo2.sourceComponentPair(), is(pair1));
        assertThat(workerTimerInfo2.workerNumber(), is(1));

        final WorkerTimerInfo workerTimerInfo3 = (WorkerTimerInfo) timerConfigs.get(2).getInfo();
        assertThat(workerTimerInfo3.sourceComponentPair(), is(pair1));
        assertThat(workerTimerInfo3.workerNumber(), is(2));

        final WorkerTimerInfo workerTimerInfo4 = (WorkerTimerInfo) timerConfigs.get(3).getInfo();
        assertThat(workerTimerInfo4.sourceComponentPair(), is(pair2));
        assertThat(workerTimerInfo4.workerNumber(), is(0));
    }

    @Test
    public void shouldNotCreateTimersWhenFeatureIsOff() {
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(false);

        streamProcessingTimerBean.startTimerService();

        verifyNoInteractions(subscriptionSourceComponentFinder, timerService);
    }

    @Test
    public void shouldProcessStreamEventsForSourceComponentPairOnTimeout() {

        final String source = "test-source";
        final String component = "test-component";
        final int workerNumber = 0;
        final SourceComponentPair pair = new SourceComponentPair(source, component);
        final WorkerTimerInfo workerTimerInfo = new WorkerTimerInfo(pair, workerNumber);

        setupTimerWithWorker(pair, 1);

        when(timer.getInfo()).thenReturn(workerTimerInfo);
        when(sufficientTimeRemainingCalculatorFactory.createNew(eq(timer), anyLong())).thenReturn(sufficientTimeRemainingCalculator);

        streamProcessingTimerBean.processStreamEvents(timer);
        verify(streamProcessingSubscriptionManager).process(eq(source), eq(component), eq(sufficientTimeRemainingCalculator));
    }

    @Test
    public void shouldLogErrorWhenProcessingFails() {

        final String source = "test-source";
        final String component = "test-component";
        final int workerNumber = 0;
        final SourceComponentPair pair = new SourceComponentPair(source, component);
        final WorkerTimerInfo workerTimerInfo = new WorkerTimerInfo(pair, workerNumber);
        final RuntimeException exception = new RuntimeException("Processing failed");

        setupTimerWithWorker(pair, 1);

        when(timer.getInfo()).thenReturn(workerTimerInfo);
        when(sufficientTimeRemainingCalculatorFactory.createNew(eq(timer), anyLong())).thenReturn(sufficientTimeRemainingCalculator);
        doThrow(exception).when(streamProcessingSubscriptionManager).process(eq(source), eq(component), eq(sufficientTimeRemainingCalculator));

        streamProcessingTimerBean.processStreamEvents(timer);

        verify(logger).error("Failed to process stream events of source: {}, component: {}, worker: {}",
                source, component, workerNumber, exception);
    }

    @Test
    public void shouldSkipProcessingWhenLockIsNotAvailable() throws Exception {

        final String source = "test-source";
        final String component = "test-component";
        final int workerNumber = 0;
        final SourceComponentPair pair = new SourceComponentPair(source, component);
        final WorkerTimerInfo workerTimerInfo = new WorkerTimerInfo(pair, workerNumber);

        setupTimerWithWorker(pair, 1);

        when(timer.getInfo()).thenReturn(workerTimerInfo);

        final Thread lockingThread = new Thread(() -> {
            streamProcessingTimerBean.processStreamEvents(timer);
        });

        when(sufficientTimeRemainingCalculatorFactory.createNew(eq(timer), anyLong())).thenAnswer(invocation -> {
            Thread.sleep(100);
            return sufficientTimeRemainingCalculator;
        });

        lockingThread.start();
        Thread.sleep(20);

        streamProcessingTimerBean.processStreamEvents(timer);

        lockingThread.join();

        verify(streamProcessingSubscriptionManager, times(1)).process(eq(source), eq(component), eq(sufficientTimeRemainingCalculator));
        verify(logger).info("Skipping timer execution for source: {}, component: {}, worker: {} - previous execution still in progress",
                source, component, workerNumber);
    }

    private void setupTimerWithWorker(final SourceComponentPair pair, final int maxThreads) {
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(singletonList(pair));
        when(streamProcessingConfig.getTimerStartWaitMilliseconds()).thenReturn(7250L);
        when(streamProcessingConfig.getTimerIntervalMilliseconds()).thenReturn(100L);
        when(streamProcessingConfig.getMaxThreads()).thenReturn(maxThreads);

        streamProcessingTimerBean.startTimerService();
    }
}
