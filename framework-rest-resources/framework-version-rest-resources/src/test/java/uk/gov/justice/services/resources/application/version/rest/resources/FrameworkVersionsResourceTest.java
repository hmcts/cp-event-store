package uk.gov.justice.services.resources.application.version.rest.resources;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersion;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionException;

import java.time.ZonedDateTime;
import java.util.List;

import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FrameworkVersionsResourceTest {

    @Mock
    private ProjectVersionsProvider projectVersionsProvider;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();
    
    @InjectMocks
    private FrameworkVersionsResource frameworkVersionsResource;

    @SuppressWarnings("unchecked")
    @Test
    public void shouldGenerateProjectVersionsRestResponseWithJsonContentType() throws Exception {

        final ProjectVersion projectVersion_1 = new ProjectVersion(
                "project_1",
                1,
                "projectVersion_1",
                ZonedDateTime.parse("2025-07-25T11:30:17Z")
        );
        final ProjectVersion projectVersion_2 = new ProjectVersion(
                "project_2",
                2,
                "projectVersion_2",
                ZonedDateTime.parse("2025-07-25T12:30:17Z")
        );
        final ProjectVersion projectVersion_3 = new ProjectVersion(
                "project_3",
                3,
                "projectVersion_3",
                ZonedDateTime.parse("2025-07-25T13:30:17Z")
        );

        when(projectVersionsProvider.findProjectVersions()).thenReturn(List.of(projectVersion_1, projectVersion_2, projectVersion_3));

        try (final Response response = frameworkVersionsResource.getFrameworkVersions()) {
            assertThat(response.getStatus(), is(200));
            assertThat(response.getHeaderString("Content-type"), is("application/json"));
            final String responseJson = response.getEntity().toString();

            with(responseJson)
                    .assertThat("$[0].projectName", is(projectVersion_1.getProjectName()))
                    .assertThat("$[0].projectVersion", is(projectVersion_1.getProjectVersion()))
                    .assertThat("$[0].displayOrder", is(projectVersion_1.getDisplayOrder()))
                    .assertThat("$[0].buildTimestamp", is("2025-07-25T11:30:17.000Z"))

                    .assertThat("$[1].projectName", is(projectVersion_2.getProjectName()))
                    .assertThat("$[1].projectVersion", is(projectVersion_2.getProjectVersion()))
                    .assertThat("$[1].displayOrder", is(projectVersion_2.getDisplayOrder()))
                    .assertThat("$[1].buildTimestamp", is("2025-07-25T12:30:17.000Z"))

                    .assertThat("$[2].projectName", is(projectVersion_3.getProjectName()))
                    .assertThat("$[2].projectVersion", is(projectVersion_3.getProjectVersion()))
                    .assertThat("$[2].displayOrder", is(projectVersion_3.getDisplayOrder()))
                    .assertThat("$[2].buildTimestamp", is("2025-07-25T13:30:17.000Z"))
            ;
        }
    }

    @Test
    public void shouldThrowProjectVersionExceptionIfProjectVersionsCannotBeSerializedToJson() throws Exception {

        final ProjectVersion projectVersion = mock(ProjectVersion.class);
        when(projectVersionsProvider.findProjectVersions()).thenReturn(List.of(projectVersion));
        when(projectVersion.getProjectName()).thenThrow(new NullPointerException());

        final ProjectVersionException projectVersionException = assertThrows(ProjectVersionException.class, () -> frameworkVersionsResource.getFrameworkVersions());
        assertThat(projectVersionException.getMessage(), is("Unable to serialize ProjectVersion List into json"));
        assertThat(projectVersionException.getCause(), is(instanceOf(JsonProcessingException.class)));
    }
}