package uk.gov.justice.services.resources.rest.streams;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;

import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.model.ResetStreamRetryCountResponse;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;

@Path("/reset-stream-retry-count")
@Produces(APPLICATION_JSON)
public class ResetStreamRetryCountResource {

    @Inject
    private ResetStreamRetryCountRequestHandler resetStreamRetryCountRequestHandler;

    @Inject
    private Logger logger;

    @PUT
    public Response resetStreamRetryCount(
            @QueryParam("streamId") final UUID streamId,
            @QueryParam("source") final String source,
            @QueryParam("component") final String component) {

        if (streamId == null) {
            return buildBadRequestResponse("Please set 'streamId' as a request parameter");
        }
        if (source == null) {
            return buildBadRequestResponse("Please set 'source' as a request parameter");
        }
        if (component == null) {
            return buildBadRequestResponse("Please set 'component' as a request parameter");
        }

        try {
            final ResetStreamRetryCountResponse resetStreamRetryCountResponse =
                    resetStreamRetryCountRequestHandler.doResetStreamRetryCount(
                            streamId,
                            source,
                            component);

            if (resetStreamRetryCountResponse.success()) {
                return ok(resetStreamRetryCountResponse, APPLICATION_JSON).build();
            } else {
                return status(BAD_REQUEST)
                        .entity(resetStreamRetryCountResponse)
                        .build();
            }

        } catch (final Exception e) {
            logger.error("Failed to reset stream retry count for streamId '%s', source '%s', component '%s'".formatted(streamId, source, component), e);
            final String errorMessage = "%s occurred while processing the request. Error message: '%s'. Please see wildfly logs for details".formatted(e.getClass().getSimpleName(), e.getMessage());
            return status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse(errorMessage))
                    .build();
        }
    }

    private Response buildBadRequestResponse(final String message) {
        return status(BAD_REQUEST)
                .entity(new ErrorResponse(message))
                .build();
    }
}
