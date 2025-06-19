package uk.gov.justice.services.eventstore.metrics.meters.statistic;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;

import java.sql.Timestamp;
import java.time.Instant;

import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamStatisticTimerBeanTest {

    @Mock
    private Logger logger;

    @Mock
    private TimerServiceManager timerServiceManager;

    @Mock
    private TimerService timerService;

    @Mock
    private MetricsConfiguration metricsConfiguration;

    @Mock
    private StreamMetricsRepository streamMetricsRepository;

    @Captor
    private ArgumentCaptor<Timestamp> timestampCaptor;

    @InjectMocks
    private StreamStatisticTimerBean streamStatisticTimerBean;

    @Test
    public void shouldSetTimerServiceWhenMetricsEnabled() {
        final long timerDelayValue = 10000L;
        final long timerIntervalValue = 60000L;

        when(metricsConfiguration.micrometerMetricsEnabled()).thenReturn(true);
        when(metricsConfiguration.statisticTimerDelayMilliseconds()).thenReturn(timerDelayValue);
        when(metricsConfiguration.statisticTimerIntervalMilliseconds()).thenReturn(timerIntervalValue);

        // run
        streamStatisticTimerBean.startTimerService();

        // verify
        verify(timerServiceManager).createIntervalTimer(
                "event-store.stream-statistic-refresh.job",
                timerDelayValue,
                timerIntervalValue,
                timerService);
    }

    @Test
    public void shouldNotSetTimerServiceWhenMetricsNotEnabled() {
        when(metricsConfiguration.micrometerMetricsEnabled()).thenReturn(false);

        // run
        streamStatisticTimerBean.startTimerService();

        // verify
        verifyNoInteractions(timerServiceManager);
    }

    @Test
    public void shouldCalculateStreamStatisticWithCorrectFreshnessLimit() {
        final long timerIntervalValue = 60000L;
        when(metricsConfiguration.statisticTimerIntervalMilliseconds()).thenReturn(timerIntervalValue);
        Instant beforeRunningTest = Instant.now().minusMillis(timerIntervalValue);

        // run
        streamStatisticTimerBean.calculateStreamStatistic();

        // verify
        verify(streamMetricsRepository).calculateStreamStatistic(timestampCaptor.capture());

        Instant capturedTimestamp = timestampCaptor.getValue().toInstant();

        Instant afterRunningTest = Instant.now().minusMillis(timerIntervalValue);


        assertTrue((capturedTimestamp.isAfter(beforeRunningTest) || capturedTimestamp.equals(beforeRunningTest))
                   && ( capturedTimestamp.isBefore(afterRunningTest) || capturedTimestamp.equals(afterRunningTest)),
                "Timestamp must be around now %s milliseconds before now".formatted(timerIntervalValue));
    }

    @Test
    public void shouldLogExceptionWhenCalculatingStreamStatistic() {
        final long timerIntervalValue = 60000L;
        final Exception exception = new RuntimeException("Test exception");

        when(metricsConfiguration.statisticTimerIntervalMilliseconds()).thenReturn(timerIntervalValue);
        doThrow(exception).when(streamMetricsRepository).calculateStreamStatistic(timestampCaptor.capture());

        // run
        streamStatisticTimerBean.calculateStreamStatistic();

        // verify
        verify(logger).warn("Error calculating stream statistic", exception);
    }
}
