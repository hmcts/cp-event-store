package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry.StreamErrorRetryBuilder.from;

import uk.gov.justice.services.common.util.UtcClock;

import org.junit.jupiter.api.Test;

public class StreamErrorRetryTest {


    @Test
    public void shouldIncrementRetryCount() throws Exception {

        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                randomUUID(),
                "some-source",
                "some-component",
                new UtcClock().now().minusMinutes(10),
                23L,
                new UtcClock().now()
        );

        final StreamErrorRetry newStreamErrorRetry = from(streamErrorRetry)
                .incrementRetryCount()
                .build();

        assertThat(newStreamErrorRetry.streamId(), is(streamErrorRetry.streamId()));
        assertThat(newStreamErrorRetry.source(), is(streamErrorRetry.source()));
        assertThat(newStreamErrorRetry.component(), is(streamErrorRetry.component()));
        assertThat(newStreamErrorRetry.occurredAt(), is(streamErrorRetry.occurredAt()));
        assertThat(newStreamErrorRetry.nextRetryTime(), is(streamErrorRetry.nextRetryTime()));

        assertThat(newStreamErrorRetry.retryCount(), is(streamErrorRetry.retryCount() + 1));

    }
}