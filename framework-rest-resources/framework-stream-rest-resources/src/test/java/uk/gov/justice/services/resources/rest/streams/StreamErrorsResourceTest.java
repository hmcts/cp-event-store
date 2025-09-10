package uk.gov.justice.services.resources.rest.streams;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamErrorsResourceTest {

    @Mock
    private StreamErrorRepository streamErrorRepository;
    @Mock
    private ObjectMapper objectMapper;
    @InjectMocks
    private StreamErrorsResource streamErrorsResource;

    @Nested
    class ErrorScenarioTest {

        @Test
        void shouldReturnBadRequestIfNoneOfStreamIdAndErrorIdProvided() {
            try(Response response = streamErrorsResource.findBy(null, null)) {
                assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
                assertTrue(response.getEntity() instanceof ErrorResponse);
                assertThat(((ErrorResponse) response.getEntity()).errorMessage(), is("Please set either 'streamId' or 'errorId' as request parameters"));
            }
        }

        @Test
        void shouldReturnBadRequestIfBothStreamIdAndErrorIdProvided() {
            try(Response response = streamErrorsResource.findBy(randomUUID(), randomUUID())) {
                assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
                assertTrue(response.getEntity() instanceof ErrorResponse);
                assertThat(((ErrorResponse) response.getEntity()).errorMessage(), is("Please set either 'streamId' or 'errorId' as request parameters, not both"));
            }
        }

        @Test
        void shouldReturnInternalServerErrorOnException() {
            final UUID streamId = randomUUID();
            doThrow(new RuntimeException()).when(streamErrorRepository).findAllByStreamId(streamId);

            Response response = streamErrorsResource.findBy(streamId, null);

            assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
            assertTrue(response.getEntity() instanceof ErrorResponse);
        }
    }

    @Nested
    class SuccessScenarioTest {

        @Test
        void shouldReturnListStreamErrorsGivenStreamId() throws Exception {
            final UUID streamId = randomUUID();
            final List<StreamError> streamErrors = List.of(mock(StreamError.class));
            when(streamErrorRepository.findAllByStreamId(streamId)).thenReturn(streamErrors);
            when(objectMapper.writeValueAsString(streamErrors)).thenReturn("{}");

            try(Response response = streamErrorsResource.findBy(streamId, null)) {
                assertThat(response.getStatus(), is(200));
                assertThat(response.getHeaderString("Content-type"), is("application/json"));
                final String streamErrorsFromResponseJson = (String) response.getEntity();
                assertThat(streamErrorsFromResponseJson, is("{}"));
            }
        }

        @Test
        void shouldReturnListStreamErrorsGivenErrorId() throws Exception {
            final UUID errorId = randomUUID();
            final StreamError streamError = mock(StreamError.class);
            when(streamErrorRepository.findByErrorId(errorId)).thenReturn(of(streamError));
            when(objectMapper.writeValueAsString(List.of(streamError))).thenReturn("{}");

            try(Response response = streamErrorsResource.findBy(null, errorId)) {
                assertThat(response.getStatus(), is(200));
                assertThat(response.getHeaderString("Content-type"), is("application/json"));
                final String streamErrorsFromResponseJson = (String) response.getEntity();
                assertThat(streamErrorsFromResponseJson, is("{}"));
            }
        }
    }
} 