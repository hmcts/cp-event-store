package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.util.List;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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

    @InjectMocks
    private StreamProcessingTimerBean streamProcessingTimerBean;

    @Test
    public void shouldCreateTimerForEachSourceComponentPairOnStartup() {

        final long timerStartWaitMilliseconds = 7250L;
        final long timerIntervalMilliseconds = 1000L;

        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component-1");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component-2");
        final List<SourceComponentPair> pairs = asList(pair1, pair2);

        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(pairs);
        when(streamProcessingConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(streamProcessingConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        streamProcessingTimerBean.startTimerService();

        verify(timerService, times(2)).createIntervalTimer(
                eq(timerStartWaitMilliseconds),
                eq(timerIntervalMilliseconds),
                any(TimerConfig.class)
        );
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
        final SourceComponentPair pair = new SourceComponentPair(source, component);

        when(timer.getInfo()).thenReturn(pair);
        when(sufficientTimeRemainingCalculatorFactory.createNew(eq(timer), anyLong())).thenReturn(sufficientTimeRemainingCalculator);

        streamProcessingTimerBean.processStreamEvents(timer);
        verify(streamProcessingSubscriptionManager).process(eq(source), eq(component), eq(sufficientTimeRemainingCalculator));
    }

    @Test
    public void shouldLogErrorWhenProcessingFails() {

        final String source = "test-source";
        final String component = "test-component";
        final SourceComponentPair pair = new SourceComponentPair(source, component);
        final RuntimeException exception = new RuntimeException("Processing failed");

        when(timer.getInfo()).thenReturn(pair);
        when(sufficientTimeRemainingCalculatorFactory.createNew(eq(timer), anyLong())).thenReturn(sufficientTimeRemainingCalculator);
        doThrow(exception).when(streamProcessingSubscriptionManager).process(eq(source), eq(component), eq(sufficientTimeRemainingCalculator));

        streamProcessingTimerBean.processStreamEvents(timer);

        verify(logger).error("Failed to process stream events of source: {}, component: {}", source, component, exception);
    }
}
