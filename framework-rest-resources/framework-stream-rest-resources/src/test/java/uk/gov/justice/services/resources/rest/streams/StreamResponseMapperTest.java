package uk.gov.justice.services.resources.rest.streams;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.rest.streams.model.StreamResponse;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class StreamResponseMapperTest {


    @Test
    void mapEntityToRestModel() {
        final StreamStatus streamStatus1 = new StreamStatus(UUID.randomUUID(), 1L, "listing", "EVENT_LISTENER", empty(), empty(), new UtcClock().now(), 2L, false);
        final StreamStatus streamStatus2 = new StreamStatus(UUID.randomUUID(), 2L, "progression", "EVENT_INDEXER", of(UUID.randomUUID()), of(2L), new UtcClock().now(), 4L, true);
        final StreamResponseMapper streamResponseMapper = new StreamResponseMapper();

        final List<StreamResponse> streams = streamResponseMapper.map(List.of(streamStatus1, streamStatus2));
        assertThat(streams.size(), is(2));
        assertThat(streams.get(0).streamId(), is(streamStatus1.streamId()));
        assertThat(streams.get(0).updatedAt(), is(streamStatus1.updatedAt().toString()));
        assertThat(streams.get(0).component(), is("EVENT_LISTENER"));
        assertThat(streams.get(0).source(), is("listing"));
        assertThat(streams.get(0).position(), is(1L));
        assertThat(streams.get(0).lastKnownPosition(), is(2L));
        assertThat(streams.get(0).upToDate(), is(false));
        assertNull(streams.get(0).errorId());
        assertNull(streams.get(0).errorPosition());

        assertThat(streams.get(1).streamId(), is(streamStatus2.streamId()));
        assertThat(streams.get(1).updatedAt(), is(streamStatus2.updatedAt().toString()));
        assertThat(streams.get(1).component(), is("EVENT_INDEXER"));
        assertThat(streams.get(1).source(), is("progression"));
        assertThat(streams.get(1).position(), is(2L));
        assertThat(streams.get(1).lastKnownPosition(), is(4L));
        assertThat(streams.get(1).upToDate(), is(true));
        assertThat(streams.get(1).errorId(), is(streamStatus2.streamErrorId().get()));
        assertThat(streams.get(1).errorPosition(), is(streamStatus2.streamErrorPosition().get()));
    }

    @Test
    void shouldReturnEmptyList() {
        final StreamResponseMapper streamResponseMapper = new StreamResponseMapper();

        final List<StreamResponse> streams = streamResponseMapper.map(List.of());
        assertThat(streams.size(), is(0));
    }
}