package uk.gov.justice.services.event.buffer.core.resource;

import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.event.buffer.core.resource.model.ErrorResponse;
import uk.gov.justice.services.event.buffer.core.resource.model.Stream;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/streams")
@Produces(MediaType.APPLICATION_JSON)
public class StreamsResource {
    private static final String INVALID_PARAM_MESSAGE = "Invalid or missing errorHash query parameter";
    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @GET
    public Response findBy(@QueryParam("errorHash") String errorHash) {
        if (errorHash == null || errorHash.isEmpty()) {
            return buildBadRequestResponse();
        }

        try {
            final List<Stream> streamResponses = newStreamStatusRepository.findBy(errorHash)
                    .stream().map(this::mapToStream).toList();
            return Response.ok(streamResponses).build();
        } catch (Exception e) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request"))
                    .build();
        }
    }

    private Stream mapToStream(StreamStatus streamStatus) {
        return new Stream(streamStatus.streamId(), streamStatus.position(),
                streamStatus.latestKnownPosition(), streamStatus.source(),
                streamStatus.component(), streamStatus.updatedAt().toString(),
                streamStatus.isUpToDate(), streamStatus.streamErrorId(), streamStatus.streamErrorPosition());
    }

    private Response buildBadRequestResponse() {
        return Response.status(BAD_REQUEST)
                .entity(new ErrorResponse(INVALID_PARAM_MESSAGE))
                .build();
    }
}
