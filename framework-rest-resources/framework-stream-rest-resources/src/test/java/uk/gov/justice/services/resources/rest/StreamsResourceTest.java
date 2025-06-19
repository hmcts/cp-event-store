package uk.gov.justice.services.resources.rest;

import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.repository.StreamStatusReadRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.model.StreamResponse;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamsResourceTest {

    @Mock
    private StreamStatusReadRepository repository;

    @InjectMocks
    private StreamsResource streamsResource;

    @ParameterizedTest
    @NullSource
    @EmptySource
    void shouldReturnBadRequestGivenInvalidErrorHash(String errorHash) {
        final Response response = streamsResource.findBy(errorHash);

        assertThat(response.getStatus(), is(400));
        assertTrue(response.getEntity() instanceof ErrorResponse);
        final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
        assertThat(errorResponse.errorMessage(), is("Invalid or missing errorHash query parameter"));
    }

    @Test
    void shouldReturnSuccessResponseGivenStreamsFoundWithErrorHash() {
        final String errorHash = "hash-1";
        final StreamStatus streamStatus1 = new StreamStatus(UUID.randomUUID(), 1L, "listing", "EVENT_LISTENER", empty(), empty(), new UtcClock().now(), 2L, false);
        final StreamStatus streamStatus2 = new StreamStatus(UUID.randomUUID(), 2L, "progression", "EVENT_INDEXER", of(UUID.randomUUID()), of(2L), new UtcClock().now(), 4L, true);
        when(repository.findBy(errorHash)).thenReturn(List.of(streamStatus1, streamStatus2));

        try(final Response response = streamsResource.findBy(errorHash.toString())) {
            assertThat(response.getStatus(), is(200));
            final List<StreamResponse> streams = (List<StreamResponse>) response.getEntity();
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
    }

    @Test
    void shouldReturnEmptyListGivenNoStreamsFoundWithErrorHash() {
        final String errorHash = "hash-1";
        when(repository.findBy(errorHash)).thenReturn(List.of());

        try(final Response response = streamsResource.findBy(errorHash)) {
            assertThat(response.getStatus(), is(200));
            final List<StreamResponse> streams = (List) response.getEntity();
            assertThat(streams.size(), is(0));
        }
    }
}