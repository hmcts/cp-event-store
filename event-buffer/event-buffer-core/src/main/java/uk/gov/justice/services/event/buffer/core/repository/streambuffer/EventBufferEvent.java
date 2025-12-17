package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.lang.Math.toIntExact;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.UUID;

public class EventBufferEvent implements Comparable<EventBufferEvent> {
    private final UUID streamId;
    private final Long position;
    private final String event;
    private final String source;
    private final String component;
    private final ZonedDateTime bufferedAt;

    public EventBufferEvent(
            final UUID streamId,
            final long position,
            final String event,
            final String source,
            final String component,
            final ZonedDateTime bufferedAt) {
        this.streamId = streamId;
        this.position = position;
        this.event = event;
        this.source = source;
        this.component = component;
        this.bufferedAt = bufferedAt;
    }

    public UUID getStreamId() {
        return streamId;
    }

    public Long getPosition() {
        return position;
    }

    public String getEvent() {
        return event;
    }

    public String getSource() {
        return source;
    }

    public String getComponent() {
        return component;
    }

    public ZonedDateTime getBufferedAt() {
        return bufferedAt;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof final EventBufferEvent that)) return false;
        return position == that.position && Objects.equals(streamId, that.streamId) && Objects.equals(event, that.event) && Objects.equals(source, that.source) && Objects.equals(component, that.component) && Objects.equals(bufferedAt, that.bufferedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, position, event, source, component, bufferedAt);
    }

    @Override
    public String toString() {
        return "EventBufferEvent{" +
                "streamId=" + streamId +
                ", position=" + position +
                ", event='" + event + '\'' +
                ", source='" + source + '\'' +
                ", component='" + component + '\'' +
                ", bufferedAt=" + bufferedAt +
                '}';
    }

    @Override
    public int compareTo(final EventBufferEvent eventBufferEvent) {
        return toIntExact(this.getPosition() - eventBufferEvent.getPosition());
    }
}
