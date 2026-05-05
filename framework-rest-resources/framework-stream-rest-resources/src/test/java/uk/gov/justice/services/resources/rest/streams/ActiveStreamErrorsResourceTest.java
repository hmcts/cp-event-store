package uk.gov.justice.services.resources.rest.streams;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.ActiveStreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.ActiveStreamErrorsRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.List;

import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class ActiveStreamErrorsResourceTest {

    @Mock
    private ActiveStreamErrorsRepository activeStreamErrorsRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Logger logger;

    @InjectMocks
    private ActiveErrorsResource activeErrorsResource;

    @Nested
    class SuccessScenarioTest {

        @Test
        void shouldReturnListOfActiveErrors() throws Exception {

            final List<ActiveStreamError> activeStreamErrors = List.of(mock(ActiveStreamError.class));
            final String activeErrorsJson = """
                    {"some": "json"}
                    """;

            when(activeStreamErrorsRepository.getActiveStreamErrors()).thenReturn(activeStreamErrors);
            when(objectMapper.writeValueAsString(activeStreamErrors)).thenReturn(activeErrorsJson);

            try (final Response response = activeErrorsResource.findActiveErrors()) {
                assertThat(response.getStatus(), is(200));
                assertThat(response.getHeaderString("Content-type"), is("application/json"));
                final String streamErrorsFromResponseJson = (String) response.getEntity();
                assertThat(streamErrorsFromResponseJson, is(activeErrorsJson));
            }
        }

        @Nested
        class ErrorScenarioTest {

            @Test
            void shouldReturn500InternalServerErrorIfFindingActiveErrorsFails() {

                final NullPointerException nullPointerException = new NullPointerException("Ooops");

                when(activeStreamErrorsRepository.getActiveStreamErrors()).thenThrow(nullPointerException);

                try (final Response response = activeErrorsResource.findActiveErrors()) {

                    assertThat(response.getStatus(), is(INTERNAL_SERVER_ERROR.getStatusCode()));
                    assertThat(response.getEntity(), is(instanceOf(ErrorResponse.class)));
                    assertThat(((ErrorResponse) response.getEntity()).errorMessage(), is("An error occurred while processing the request. Please see server logs for details."));

                    verify(logger).error("Failed to find List of active stream errors", nullPointerException);
                }
            }
        }
    }
}