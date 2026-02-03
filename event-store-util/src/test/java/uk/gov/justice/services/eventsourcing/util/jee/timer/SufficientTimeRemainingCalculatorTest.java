package uk.gov.justice.services.eventsourcing.util.jee.timer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ejb.Timer;

import org.junit.jupiter.api.Test;

public class SufficientTimeRemainingCalculatorTest {

    @Test
    public void shouldCalculateIfThereIsSufficientTimeRemainingToProcessNextEvent() throws Exception {

        final Long timeBetweenRuns = 10L;

        final Timer timer = mock(Timer.class);
        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = new SufficientTimeRemainingCalculator(timer, timeBetweenRuns);

        when(timer.getTimeRemaining()).thenReturn(11L, 10L, 9L);

        assertThat(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining(), is(true));
        assertThat(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining(), is(false));
        assertThat(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining(), is(false));
    }
}