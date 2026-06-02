package uk.gov.justice.services.resources.application.version.rest.resources;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersion;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionException;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/versions")
@Produces(APPLICATION_JSON)
public class FrameworkVersionsResource {

    @Inject
    private ProjectVersionsProvider projectVersionsProvider;

    @Inject
    private ObjectMapper objectMapper;

    @GET
    public Response getFrameworkVersions() {

        final List<ProjectVersion> projectVersions = projectVersionsProvider.findProjectVersions();

        try {
            final String json = createJsonStringAsWildflyCanNotParseZoneDateTime(projectVersions);
            return Response.ok(json, APPLICATION_JSON).build();
        } catch (final JsonProcessingException e) {
            throw new ProjectVersionException("Unable to serialize ProjectVersion List into json", e);
        }

    }

    private String createJsonStringAsWildflyCanNotParseZoneDateTime(final List<ProjectVersion> projectVersions) throws JsonProcessingException {
        return objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(projectVersions);
    }
}
