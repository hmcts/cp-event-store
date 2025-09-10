package uk.gov.justice.services.resources.rest.streams;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.rest.streams.model.StreamResponse;

import java.util.List;

public class StreamResponseMapper {

    public List<StreamResponse> map(List<StreamStatus> streamStatuses) {
        return streamStatuses.stream()
                .map(this::map).toList();
    }

    private StreamResponse map(final StreamStatus streamStatus) {
        return new StreamResponse(streamStatus.streamId(), streamStatus.position(),
                streamStatus.latestKnownPosition(), streamStatus.source(),
                streamStatus.component(), streamStatus.updatedAt().toString(),
                streamStatus.isUpToDate(), streamStatus.streamErrorId().orElse(null),
                streamStatus.streamErrorPosition().orElse(null));
    }
}
