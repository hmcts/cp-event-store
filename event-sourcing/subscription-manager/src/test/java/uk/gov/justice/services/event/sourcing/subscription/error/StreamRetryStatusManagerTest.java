package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamRetryConfiguration;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamRetryStatusManagerTest {

    @Mock
    private StreamErrorRetryRepository streamErrorRetryRepository;


    @Mock
    private RetryTimeCalculator retryTimeCalculator;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private StreamRetryStatusManager streamRetryStatusManager;

    @Test
    public void shouldCalculateNumberOfRetriesAndNextRetryTimeAndUpdateStreamErrorRetryTable() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long retryCount = 5L;
        final long retryDelay = 1_000L;
        final BigDecimal retryDelayMultiplier = new BigDecimal("2.0");

        final ZonedDateTime now = ZonedDateTime.of(2026, 2, 20, 12, 0, 0, 0, UTC);

        final ZonedDateTime nextRetryTime = now.plusMinutes(1);

        when(streamErrorRetryRepository.getRetryCount(streamId, source, component)).thenReturn(retryCount);
        when(clock.now()).thenReturn(now);
        when(retryTimeCalculator.calculateNextRetryTime(retryCount + 1, now)).thenReturn(nextRetryTime);

        streamRetryStatusManager.updateStreamRetryCountAndNextRetryTime(streamId, source, component);

        final ArgumentCaptor<StreamErrorRetry> streamErrorRetryCaptor = forClass(StreamErrorRetry.class);

        verify(streamErrorRetryRepository).upsert(streamErrorRetryCaptor.capture());

        final StreamErrorRetry streamErrorRetry = streamErrorRetryCaptor.getValue();

        final long expectedDelay = (retryCount + 1) * retryDelay * retryDelayMultiplier.longValue();

        assertThat(streamErrorRetry.streamId(), is(streamId));
        assertThat(streamErrorRetry.source(), is(source));
        assertThat(streamErrorRetry.component(), is(component));
        assertThat(streamErrorRetry.retryCount(), is(retryCount + 1));
        assertThat(streamErrorRetry.nextRetryTime(), is(nextRetryTime));
    }

    @Test
    public void shouldRemoveStreamRetryStatus() throws Exception {
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        streamRetryStatusManager.removeStreamRetryStatus(streamId, source, component);

        verify(streamErrorRetryRepository).remove(streamId, source, component);
    }
}