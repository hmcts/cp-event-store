package uk.gov.justice.services.resources;

import java.util.Set;
import org.junit.jupiter.api.Test;

import uk.gov.justice.services.resources.rest.streams.ActiveErrorsResource;
import uk.gov.justice.services.resources.rest.streams.StreamErrorsResource;
import uk.gov.justice.services.resources.rest.streams.StreamsResource;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

class FrameworkApplicationTest {

    private final FrameworkApplication frameworkApplication = new FrameworkApplication();

    @Test
    void validateConfiguredResourceClasses() {
        final Set<Class<?>> resourceClasses = frameworkApplication.getClasses();

        assertThat(resourceClasses.size(), is(3));
        assertThat(resourceClasses, hasItem(StreamsResource.class));
        assertThat(resourceClasses, hasItem(StreamErrorsResource.class));
        assertThat(resourceClasses, hasItem(ActiveErrorsResource.class));
    }
}