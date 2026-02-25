package uk.gov.justice.services.eventsourcing.source.api.resource;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.noContent;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;

import static java.lang.String.format;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;

@Path("/event")
public class EventResource {

    @Inject
    private NextEventReader nextEventReader;

    @Inject
    private ObjectToJsonValueConverter converter;

    @Inject
    private Logger logger;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response nextEvent(@QueryParam("streamId") final UUID streamId,
                              @QueryParam("afterPosition") final long afterPosition) {

        try {
            return nextEventReader.read(streamId, afterPosition, null)
                    .map(jsonEnvelope -> ok(converter.convert(jsonEnvelope)).build())
                    .orElseGet(() -> noContent().build());
        } catch (final Exception e) {
            logger.error(format("Failed to read next event: streamId '%s', afterPosition '%d'", streamId, afterPosition), e);
            return status(INTERNAL_SERVER_ERROR).build();
        }
    }
}
