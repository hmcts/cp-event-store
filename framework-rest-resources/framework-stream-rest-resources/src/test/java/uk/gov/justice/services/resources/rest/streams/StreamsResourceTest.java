package uk.gov.justice.services.resources.rest.streams;

import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.repository.StreamStatusReadRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.streams.model.StreamResponse;

import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.resources.rest.streams.StreamsResource.INVALID_PARAM_MESSAGE;

@ExtendWith(MockitoExtension.class)
class StreamsResourceTest {

    @Mock
    private StreamStatusReadRepository repository;

    @Mock
    private StreamResponseMapper streamResponseMapper;

    @InjectMocks
    private StreamsResource streamsResource;

    @Nested
    class InvalidQueryParametersTest {

        @Test
        void shouldReturnBadRequestGivenAllQueryParametersAreEmpty() {
            final Response response = streamsResource.findBy(null, null, null);

            assertThat(response.getStatus(), is(400));
            assertTrue(response.getEntity() instanceof ErrorResponse);
            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(errorResponse.errorMessage(), is(INVALID_PARAM_MESSAGE));
        }

        @Test
        void shouldReturnBadRequestGivenEmptyErrorHashQueryParameter() {
            final Response response = streamsResource.findBy("", null, null);

            assertThat(response.getStatus(), is(400));
            assertTrue(response.getEntity() instanceof ErrorResponse);
            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(errorResponse.errorMessage(), is(INVALID_PARAM_MESSAGE));
        }

        @Test
        void shouldReturnBadRequestGivenMoreThanOneQueryParameterProvided() {
            final Response response = streamsResource.findBy("hash-1", UUID.randomUUID(), null);

            assertThat(response.getStatus(), is(400));
            assertTrue(response.getEntity() instanceof ErrorResponse);
            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(errorResponse.errorMessage(), is(INVALID_PARAM_MESSAGE));
        }

        @Test
        void shouldReturnBadRequestGivenErrorHashHasValueFalse() {
            final Response response = streamsResource.findBy(null, null, false);

            assertThat(response.getStatus(), is(400));
            assertTrue(response.getEntity() instanceof ErrorResponse);
            final ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
            assertThat(errorResponse.errorMessage(), is(INVALID_PARAM_MESSAGE));
        }
    }

    @Nested
    class ValidResponseTest {

        final StreamStatus streamStatus = new StreamStatus(UUID.randomUUID(), 1L, "listing", "EVENT_LISTENER", empty(), empty(), new UtcClock().now(), 2L, false);
        final List<StreamResponse> streamResponses = List.of(new StreamResponse(UUID.randomUUID(), 1L, 1L, "listing", "EVENT_LISTENER", new UtcClock().now().toString(), true, UUID.randomUUID(), 1L));

        @Test
        void findStreamsByErrorHash() {
            final String errorHash = "hash-1";
            when(repository.findByErrorHash(errorHash)).thenReturn(List.of(streamStatus));
            when(streamResponseMapper.map(List.of(streamStatus))).thenReturn(streamResponses);

            try(final Response response = streamsResource.findBy(errorHash, null, null)) {
                assertThat(response.getStatus(), is(200));
                final List<StreamResponse> streams = (List<StreamResponse>) response.getEntity();
                assertThat(streams, is(streamResponses));
            }
        }

        @Test
        void findStreamsByStreamId() {
            final UUID streamId = UUID.randomUUID();
            when(repository.findByStreamId(streamId)).thenReturn(List.of(streamStatus));
            when(streamResponseMapper.map(List.of(streamStatus))).thenReturn(streamResponses);

            try(final Response response = streamsResource.findBy(null, streamId, null)) {
                assertThat(response.getStatus(), is(200));
                final List<StreamResponse> streams = (List<StreamResponse>) response.getEntity();
                assertThat(streams, is(streamResponses));
            }
        }

        @Test
        void findStreamsByHasError() {
            when(repository.findErrorStreams()).thenReturn(List.of(streamStatus));
            when(streamResponseMapper.map(List.of(streamStatus))).thenReturn(streamResponses);

            try(final Response response = streamsResource.findBy(null, null, true)) {
                assertThat(response.getStatus(), is(200));
                final List<StreamResponse> streams = (List<StreamResponse>) response.getEntity();
                assertThat(streams, is(streamResponses));
            }
        }
    }
}