package uk.gov.justice.services.resources.application.version.rest.resources;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersion;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionException;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionSorter;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProjectVersionsProviderTest {

    @Spy
    private ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();

    @Mock
    private ProjectVersionsJsonProvider projectVersionsJsonProvider;

    @Mock
    private ProjectVersionSorter projectVersionSorter;
    
    @InjectMocks
    private ProjectVersionsProvider projectVersionsProvider;

    @Test
    public void shouldFindAllProjectVersionsJsonAndConvertToObjects() throws Exception {

        final String json_1 = """
                {
                    "projectName": "Framework Libraries",
                    "displayOrder": 20,
                    "projectVersion": "17.104.1",
                    "buildTimestamp": "2025-07-25T01:30:17Z"
                }
                """;
        final String json_2 = """
                {
                    "projectName": "Microservices Framework",
                    "displayOrder": 40,
                    "projectVersion": "17.104.2",
                    "buildTimestamp": "2025-07-25T02:30:17Z"
                }
                """;
        final String json_3 = """
                {
                    "projectName": "Event Store",
                    "displayOrder": 50,
                    "projectVersion": "17.104.3",
                    "buildTimestamp": "2025-07-25T03:30:17Z"
                }
                """;

        when(projectVersionsJsonProvider.getProjectVersionsJson()).thenReturn(List.of(json_1, json_2, json_3));
        when(projectVersionSorter.sortByDisplayOrder(any())).thenAnswer(i -> i.getArguments()[0]);

        final List<ProjectVersion> projectVersions = projectVersionsProvider.findProjectVersions();

        assertThat(projectVersions.size(), is(3));

        assertThat(projectVersions.get(0).getProjectName(), is("Framework Libraries"));
        assertThat(projectVersions.get(0).getDisplayOrder(), is(20));
        assertThat(projectVersions.get(0).getProjectVersion(), is("17.104.1"));
        assertThat(projectVersions.get(0).getBuildTimestamp().toInstant(), is(ZonedDateTime.of(2025, 7, 25, 1, 30, 17, 0, ZoneOffset.UTC).toInstant()));

        assertThat(projectVersions.get(1).getProjectName(), is("Microservices Framework"));
        assertThat(projectVersions.get(1).getDisplayOrder(), is(40));
        assertThat(projectVersions.get(1).getProjectVersion(), is("17.104.2"));
        assertThat(projectVersions.get(1).getBuildTimestamp().toInstant(), is(ZonedDateTime.of(2025, 7, 25, 2, 30, 17, 0, ZoneOffset.UTC).toInstant()));

        assertThat(projectVersions.get(2).getProjectName(), is("Event Store"));
        assertThat(projectVersions.get(2).getDisplayOrder(), is(50));
        assertThat(projectVersions.get(2).getProjectVersion(), is("17.104.3"));
        assertThat(projectVersions.get(2).getBuildTimestamp().toInstant(), is(ZonedDateTime.of(2025, 7, 25, 3, 30, 17, 0, ZoneOffset.UTC).toInstant()));
    }

    @Test
    public void shouldThrowProjectVersionExceptionIfJsonFailsToParse() throws Exception {

        when(projectVersionsJsonProvider.getProjectVersionsJson()).thenReturn(List.of("not-valid-json"));
        final ProjectVersionException projectVersionException = assertThrows(ProjectVersionException.class, () -> projectVersionsProvider.findProjectVersions());

        assertThat(projectVersionException.getCause(), is(instanceOf(JsonParseException.class)));
        assertThat(projectVersionException.getMessage(), is("Failed to parse json from 'version/project-version.json' file from classpath"));

    }
}