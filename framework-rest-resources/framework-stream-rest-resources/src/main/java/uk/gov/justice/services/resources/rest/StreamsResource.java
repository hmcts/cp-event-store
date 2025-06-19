package uk.gov.justice.services.resources.rest;

import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.repository.StreamStatusReadRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.model.StreamResponse;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/streams")
@Produces(MediaType.APPLICATION_JSON)
public class StreamsResource {
    private static final String INVALID_PARAM_MESSAGE = "Invalid or missing errorHash query parameter";
    @Inject
    private StreamStatusReadRepository streamStatusReadRepository;

    @GET
    public Response findBy(@QueryParam("errorHash") String errorHash) {
        if (errorHash == null || errorHash.isEmpty()) {
            return buildBadRequestResponse();
        }

        try {
            final List<StreamResponse> streamResponses = streamStatusReadRepository.findBy(errorHash)
                    .stream().map(this::mapToStream).toList();
            return Response.ok(streamResponses).build();
        } catch (Exception e) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request"))
                    .build();
        }
    }

    private StreamResponse mapToStream(StreamStatus streamStatus) {
        return new StreamResponse(streamStatus.streamId(), streamStatus.position(),
                streamStatus.latestKnownPosition(), streamStatus.source(),
                streamStatus.component(), streamStatus.updatedAt().toString(),
                streamStatus.isUpToDate(), streamStatus.streamErrorId().orElse(null), streamStatus.streamErrorPosition().orElse(null));
    }

    private Response buildBadRequestResponse() {
        return Response.status(BAD_REQUEST)
                .entity(new ErrorResponse(INVALID_PARAM_MESSAGE))
                .build();
    }
}
