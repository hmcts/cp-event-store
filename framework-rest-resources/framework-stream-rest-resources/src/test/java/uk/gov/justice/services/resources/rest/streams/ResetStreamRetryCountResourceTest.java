package uk.gov.justice.services.resources.rest.streams;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistenceException;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.model.ResetStreamRetryCountResponse;

import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ResetStreamRetryCountResourceTest {

    @Mock
    private ResetStreamRetryCountRequestHandler resetStreamRetryCountRequestHandler;

    @Mock
    private Logger logger;

    @InjectMocks
    private ResetStreamRetryCountResource resetStreamRetryCountResource;

    @Test
    public void shouldReturnHttpOkIfRetryCountSuccessfullyReset() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final ResetStreamRetryCountResponse streamRetryCountResponse = mock(ResetStreamRetryCountResponse.class);

        when(resetStreamRetryCountRequestHandler.doResetStreamRetryCount(streamId, source, component)).thenReturn(streamRetryCountResponse);
        when(streamRetryCountResponse.success()).thenReturn(true);

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(OK.getStatusCode()));
            assertThat(response.getEntity(), is(streamRetryCountResponse));
        }
    }

    @Test
    public void shouldReturnHttpBadRequestIfResettingRetryCountIsUnsuccessful() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final ResetStreamRetryCountResponse streamRetryCountResponse = mock(ResetStreamRetryCountResponse.class);

        when(resetStreamRetryCountRequestHandler.doResetStreamRetryCount(streamId, source, component)).thenReturn(streamRetryCountResponse);
        when(streamRetryCountResponse.success()).thenReturn(false);

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(BAD_REQUEST.getStatusCode()));
            assertThat(response.getEntity(), is(streamRetryCountResponse));
        }
    }

    @Test
    public void shouldReturnHttpBadRequestIfNoStreamIdParameterFound() throws Exception {

        final UUID streamId = null;
        final String source = "some-source";
        final String component = "some-component";

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(BAD_REQUEST.getStatusCode()));
            assertThat(response.getEntity(), is(instanceOf(ErrorResponse.class)));
            assertThat(((ErrorResponse)response.getEntity()).errorMessage(), is("Please set 'streamId' as a request parameter"));
        }
    }

    @Test
    public void shouldReturnHttpBadRequestIfNoSourceParameterFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = null;
        final String component = "some-component";

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(BAD_REQUEST.getStatusCode()));
            assertThat(response.getEntity(), is(instanceOf(ErrorResponse.class)));
            assertThat(((ErrorResponse)response.getEntity()).errorMessage(), is("Please set 'source' as a request parameter"));
        }
    }

    @Test
    public void shouldReturnHttpServerErrorIfResettingRetryCountThrowsException() throws Exception {

        final UUID streamId = fromString("922d19bf-b199-438f-9c70-5495840f1ca1");
        final String source = "some-source";
        final String component = "some-component";

        final StreamErrorPersistenceException streamErrorPersistenceException = new StreamErrorPersistenceException("The Norwegians are leaving");

        when(resetStreamRetryCountRequestHandler.doResetStreamRetryCount(streamId, source, component)).thenThrow(streamErrorPersistenceException);

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(INTERNAL_SERVER_ERROR.getStatusCode()));
            assertThat(response.getEntity(), is(instanceOf(ErrorResponse.class)));
            assertThat(((ErrorResponse)response.getEntity()).errorMessage(), is("StreamErrorPersistenceException occurred while processing the request. Error message: 'The Norwegians are leaving'. Please see wildfly logs for details"));

            verify(logger).error("Failed to reset stream retry count for streamId '922d19bf-b199-438f-9c70-5495840f1ca1', source 'some-source', component 'some-component'", streamErrorPersistenceException);
        }
    }

    @Test
    public void shouldReturnHttpBadRequestIfNoComponentParameterFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = null;

        try(final Response response = resetStreamRetryCountResource.resetStreamRetryCount(streamId, source, component)) {
            assertThat(response.getStatus(), is(BAD_REQUEST.getStatusCode()));
            assertThat(response.getEntity(), is(instanceOf(ErrorResponse.class)));
            assertThat(((ErrorResponse)response.getEntity()).errorMessage(), is("Please set 'component' as a request parameter"));
        }
    }
}