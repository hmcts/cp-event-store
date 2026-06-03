package uk.gov.justice.services.eventsourcing.source.api.resource;


import static uk.gov.justice.services.eventsourcing.source.api.resource.RequestValidator.validateRequest;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Direction.valueOf;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.eventsourcing.source.api.security.AccessController;
import uk.gov.justice.services.eventsourcing.source.api.service.EventsPageService;
import uk.gov.justice.services.eventsourcing.source.api.service.Page;

import java.net.MalformedURLException;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.json.JsonValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.UriInfo;

@Path("event-streams/{streamId}/{position}/{direction}/{pageSize}")
public class EventPageResource {

    @Context
    HttpHeaders headers;

    @Inject
    EventsPageService eventsPageService;

    @Inject
    AccessController accessController;

    @Inject
    ObjectToJsonValueConverter converter;

    @GET
    @Produces("application/vnd.event-source.events+json")
    public JsonValue events(
            @PathParam("streamId") final String streamId,
            @PathParam("position") final String position,
            @PathParam("direction") final String direction,
            @PathParam("pageSize") final int pageSize,
            @Context final UriInfo uriInfo) throws MalformedURLException {

        validateRequest(position, direction);

        accessController.checkAccessControl(headers);

        final Page page = eventsPageService.pageEvents(UUID.fromString(streamId), position, valueOf(direction), pageSize, uriInfo);

        return converter.convert(page);
    }
}
