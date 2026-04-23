package uk.gov.justice.services.eventsourcing.source.api.resource;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.ok;
import static jakarta.ws.rs.core.Response.status;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoverer;
import uk.gov.justice.services.eventsourcing.discovery.TransactionalDiscoverer;

import static java.lang.String.format;

import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;

@Path("/events-discovery")
public class EventDiscoveryResource {

    @Inject
    @TransactionalDiscoverer
    private EventSubscriptionDiscoverer eventSubscriptionDiscoverer;

    @Inject
    private ObjectToJsonValueConverter converter;

    @Inject
    private Logger logger;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response discoverEvents(@QueryParam("afterEventId") final UUID afterEventId,
                                   @QueryParam("batchSize") final int batchSize) {

        try {
            final DiscoveryResult discoveryResult = eventSubscriptionDiscoverer.discoverNewEvents(
                    Optional.ofNullable(afterEventId),
                    batchSize, null);

            return ok(converter.convert(discoveryResult)).build();
        } catch (final Exception e) {
            logger.error(format("Failed to discover events: afterEventId '%s', batchSize '%d'", afterEventId, batchSize), e);
            return status(INTERNAL_SERVER_ERROR).build();
        }
    }
}
