package uk.gov.justice.services.resources.rest.streams;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ObjectUtils;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.resources.repository.StreamStatusReadRepository;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;
import uk.gov.justice.services.resources.rest.streams.model.StreamResponse;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;

@Path("/streams")
@Produces(MediaType.APPLICATION_JSON)
public class StreamsResource {
    static final String INVALID_PARAM_MESSAGE = "Exactly one query parameter(errorHash/streamId/hasError) must be provided. Accepted values for errorHash: true";
    @Inject
    private StreamStatusReadRepository streamStatusReadRepository;

    @Inject
    private StreamResponseMapper streamResponseMapper;

    @GET
    public Response findBy(@QueryParam("errorHash") String errorHash,
                           @QueryParam("streamId") UUID streamId,
                           @QueryParam("hasError") Boolean hasError) {
        if (!validQueryParameters(errorHash, streamId, hasError)) {
            return buildBadRequestResponse();
        }

        try {
            final List<StreamStatus> filteredStreamStatuses = findByStreamsBy(errorHash, streamId);
            final List<StreamResponse> streamResponses = streamResponseMapper.map(filteredStreamStatuses);

            return Response.ok(streamResponses).build();

        } catch (Exception e) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request"))
                    .build();
        }
    }

    private List<StreamStatus> findByStreamsBy(String errorHash, UUID streamId) {
        if (isNotEmpty(errorHash)) {
            return streamStatusReadRepository.findByErrorHash(errorHash);
        } else if (isNotEmpty(streamId)) {
            return streamStatusReadRepository.findByStreamId(streamId);
        } else {
            return streamStatusReadRepository.findErrorStreams();
        }
    }

    private boolean validQueryParameters(String errorHash, UUID streamId, Boolean hasError) {
        return providedOnlyOneParameter(errorHash, streamId, hasError) && validErrorHashValue(hasError);
    }

    private boolean validErrorHashValue(Boolean hasError) {
        return hasError == null || hasError;
    }

    private boolean providedOnlyOneParameter(String errorHash, UUID streamId, Boolean hasError) {
        final int noOfProvidedQueryParameters = Stream.of(errorHash, streamId, hasError)
                .filter(ObjectUtils::isNotEmpty)
                .toList().size();

        return noOfProvidedQueryParameters == 1;
    }

    private Response buildBadRequestResponse() {
        return Response.status(BAD_REQUEST)
                .entity(new ErrorResponse(INVALID_PARAM_MESSAGE))
                .build();
    }
}
