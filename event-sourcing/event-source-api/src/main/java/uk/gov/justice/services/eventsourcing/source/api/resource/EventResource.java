package uk.gov.justice.services.eventsourcing.source.api.resource;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.noContent;
import static jakarta.ws.rs.core.Response.ok;
import static jakarta.ws.rs.core.Response.status;

import uk.gov.justice.services.eventsourcing.eventreader.TransactionalReader;
import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;

import static java.lang.String.format;

import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;

@Path("/event")
public class EventResource {

    @Inject
    @TransactionalReader
    private NextEventReader nextEventReader;

    @Inject
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    private Logger logger;

    @GET
    @Path("/{streamId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response nextEvent(@PathParam("streamId") final UUID streamId,
                              @QueryParam("afterPosition") final long afterPosition) {

        try {
            return nextEventReader.read(streamId, afterPosition, null)
                    .map(jsonEnvelope -> ok(jsonObjectEnvelopeConverter.fromEnvelope(jsonEnvelope)).build())
                    .orElseGet(() -> noContent().build());
        } catch (final Exception e) {
            logger.error(format("Failed to read next event: streamId '%s', afterPosition '%d'", streamId, afterPosition), e);
            return status(INTERNAL_SERVER_ERROR).build();
        }
    }
}
