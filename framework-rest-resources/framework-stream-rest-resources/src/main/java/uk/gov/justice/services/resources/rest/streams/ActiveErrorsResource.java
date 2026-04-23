package uk.gov.justice.services.resources.rest.streams;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.ActiveStreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.ActiveStreamErrorsRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

@Path("/errors/active-summary")
@Produces(APPLICATION_JSON)
public class ActiveErrorsResource {

    @Inject
    private ActiveStreamErrorsRepository activeStreamErrorsRepository;

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private Logger logger;

    @GET
    public Response findActiveErrors() {
        try {
            final List<ActiveStreamError> activeStreamErrors = activeStreamErrorsRepository.getActiveStreamErrors();
            
            final String activeErrorsJson = objectMapper.writeValueAsString(activeStreamErrors);
            return Response.ok(activeErrorsJson, APPLICATION_JSON).build();
        } catch (final Exception e) {
            logger.error("Failed to find List of active stream errors", e);
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request. Please see server logs for details."))
                    .build();
        }
    }
}