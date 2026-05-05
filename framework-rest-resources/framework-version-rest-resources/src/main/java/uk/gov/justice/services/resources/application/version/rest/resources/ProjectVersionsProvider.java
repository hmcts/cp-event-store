package uk.gov.justice.services.resources.application.version.rest.resources;

import static java.lang.String.format;
import static uk.gov.justice.services.resources.application.version.rest.resources.ProjectVersionsJsonProvider.PROJECT_VERSION_JSON_FILE;

import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersion;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionException;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionSorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ProjectVersionsProvider {

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private ProjectVersionSorter projectVersionSorter;

    @Inject
    public ProjectVersionsJsonProvider projectVersionsJsonProvider;

    public List<ProjectVersion> findProjectVersions() {

        final ArrayList<ProjectVersion> projectVersions = new ArrayList<>();
        try {
            for (final String json : projectVersionsJsonProvider.getProjectVersionsJson()) {
                final ProjectVersion projectVersion = objectMapper.readValue(json, ProjectVersion.class);
                projectVersions.add(projectVersion);
            }
        } catch (final IOException e) {
            throw new ProjectVersionException(format("Failed to parse json from '%s' file from classpath", PROJECT_VERSION_JSON_FILE), e);
        }
        return projectVersionSorter.sortByDisplayOrder(projectVersions);
    }
}
