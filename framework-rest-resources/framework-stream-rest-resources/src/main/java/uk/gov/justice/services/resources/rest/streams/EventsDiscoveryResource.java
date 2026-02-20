package uk.gov.justice.services.resources.rest.streams;

import static java.util.Optional.ofNullable;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

@Path("/events-discovery")
@Produces(APPLICATION_JSON)
public class EventsDiscoveryResource {

    @Inject
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private Logger logger;

    @GET
    public Response discoverEvents(@QueryParam("afterEventId") final UUID afterEventId, @QueryParam("limit") final int limit) {
        try {
            final DiscoveryResult discoveryResult = eventSubscriptionDiscoveryBean.discoverNewEvents(ofNullable(afterEventId), limit);
            final String discoveryResultJson = objectMapper.writeValueAsString(discoveryResult);
            return Response.ok(discoveryResultJson, APPLICATION_JSON).build();
        } catch (final Exception e) {
            logger.error("Failed to discover events with afterEventId: '{}' and limit: '{}'", afterEventId, limit, e);
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("An error occurred while processing the request"))
                    .build();
        }
    }
}
