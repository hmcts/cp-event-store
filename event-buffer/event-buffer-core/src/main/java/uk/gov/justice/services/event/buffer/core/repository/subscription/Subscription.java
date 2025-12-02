package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.util.Objects;
import java.util.UUID;

/**
 * Entity to represent event subscription
 */
public class Subscription {

    private final UUID streamId;
    private final Long position;
    private final String source;
    private final String component;

    public Subscription(final UUID streamId, final Long position, final String source, final String component) {
        this.streamId = streamId;
        this.position = position;
        this.source = source;
        this.component = component;
    }

    public UUID getStreamId() {
        return streamId;
    }

    public Long getPosition() {
        return position;
    }

    public String getSource() {
        return source;
    }

    public String getComponent() { return component; }

    @Override
    public String toString() {
        return "Subscription{" +
                "streamId=" + streamId +
                ", position=" + position +
                ", source='" + source + '\'' +
                ", component='" + component + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Subscription that = (Subscription) o;
        return position == that.position &&
                Objects.equals(streamId, that.streamId) &&
                Objects.equals(source, that.source) &&
                Objects.equals(component, that.component);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, position, source, component);
    }
}
