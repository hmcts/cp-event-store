package uk.gov.justice.services.resources.rest.streams;

import static java.util.Collections.emptyList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/stream-errors")
@Produces(APPLICATION_JSON)
public class StreamErrorsResource {

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Inject
    private ObjectMapper objectMapper;

    @GET
    public Response findBy(@QueryParam("streamId") UUID streamId, @QueryParam("errorId") UUID errorId) {
        if (streamId != null && errorId != null) {
            return buildBadRequestResponse("Please set either 'streamId' or 'errorId' as request parameters, not both");
        }
        if (streamId == null && errorId == null) {
            return buildBadRequestResponse("Please set either 'streamId' or 'errorId' as request parameters");
        }

        try {
            final List<StreamError> streamErrors = findStreamErrors(streamId, errorId);
            final String streamErrorsJson = objectMapper.writeValueAsString(streamErrors);
            return Response.ok(streamErrorsJson, APPLICATION_JSON).build();
        } catch (Exception e) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request"))
                    .build();
        }
    }

    private Response buildBadRequestResponse(String message) {
        return Response.status(BAD_REQUEST)
                .entity(new ErrorResponse(message))
                .build();
    }

    private List<StreamError> findStreamErrors(final UUID streamId, final UUID errorId) {
        if (streamId != null) {
            return streamErrorRepository.findAllByStreamId(streamId);
        } else if (errorId != null) {
            return streamErrorRepository.findByErrorId(errorId)
                    .map(List::of)
                    .orElse(List.of());
        }

        return emptyList();
    }
} 