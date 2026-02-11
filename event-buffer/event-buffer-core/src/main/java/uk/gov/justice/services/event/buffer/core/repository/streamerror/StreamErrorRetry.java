package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.time.ZonedDateTime;
import java.util.UUID;

public record StreamErrorRetry(
        UUID streamId,
        String source,
        String component,
        ZonedDateTime occurredAt,
        Long retryCount,
        ZonedDateTime nextRetryTime
        ) {

        public static final class StreamErrorRetryBuilder {
                private UUID streamId;
                private String source;
                private String component;
                private ZonedDateTime occurredAt;
                private Long retryCount;
                private ZonedDateTime nextRetryTime;

                private StreamErrorRetryBuilder() {
                }

                public static StreamErrorRetryBuilder streamErrorRetry() {
                        return new StreamErrorRetryBuilder();
                }

                public static StreamErrorRetryBuilder from(final StreamErrorRetry other) {

                        final StreamErrorRetryBuilder streamErrorRetryBuilder = new StreamErrorRetryBuilder();
                        streamErrorRetryBuilder.streamId = other.streamId();
                        streamErrorRetryBuilder.source = other.source();
                        streamErrorRetryBuilder.component = other.component();
                        streamErrorRetryBuilder.occurredAt = other.occurredAt();
                        streamErrorRetryBuilder.retryCount = other.retryCount();
                        streamErrorRetryBuilder.nextRetryTime = other.nextRetryTime();

                        return streamErrorRetryBuilder;
                }

                public StreamErrorRetryBuilder incrementRetryCount() {
                        this.retryCount++;
                        return this;
                }

                public StreamErrorRetryBuilder withOccurredAt(final ZonedDateTime occurredAt) {
                        this.occurredAt = occurredAt;
                        return this;
                }
                
                public StreamErrorRetryBuilder withNextRetryTime(final ZonedDateTime nextRetryTime) {
                        this.nextRetryTime = nextRetryTime;
                        return this;
                }

                public StreamErrorRetry build() {
                        return new StreamErrorRetry(streamId, source, component, occurredAt, retryCount, nextRetryTime);
                }
        }
}
