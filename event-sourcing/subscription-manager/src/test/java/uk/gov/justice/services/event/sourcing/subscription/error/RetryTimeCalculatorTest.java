package uk.gov.justice.services.event.sourcing.subscription.error;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamRetryConfiguration;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RetryTimeCalculatorTest {

    @Mock
    private StreamRetryConfiguration streamRetryConfiguration;

    @InjectMocks
    private RetryTimeCalculator retryTimeCalculator;

    @Test
    public void shouldCalculateRetryTimeAsNowPlusRetryDelayMultipliedByRetryCountMultipliedByRetryDelayMultiplier() throws Exception {

        final Long retryDelayMilliseconds = 1_000L;
        final long retryCount = 6L;
        final BigDecimal retryDelayMultiplier = new BigDecimal("5.0");


        final ZonedDateTime now = new UtcClock().now();

        // 1 (second) * 6 * 5 should make a delay of 30 seconds
        final ZonedDateTime thirtySecondsTime = now.plusSeconds(30);

        when(streamRetryConfiguration.getRetryDelayMilliseconds()).thenReturn(retryDelayMilliseconds);
        when(streamRetryConfiguration.getRetryDelayMultiplier()).thenReturn(retryDelayMultiplier);

        assertThat(retryTimeCalculator.calculateNextRetryTime(retryCount, now), is(thirtySecondsTime));
    }
}