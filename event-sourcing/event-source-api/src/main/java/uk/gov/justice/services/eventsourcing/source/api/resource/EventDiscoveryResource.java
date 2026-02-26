package uk.gov.justice.services.eventsourcing.source.api.resource;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoverer;
import uk.gov.justice.services.eventsourcing.discovery.TransactionalDiscoverer;

import static java.lang.String.format;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
