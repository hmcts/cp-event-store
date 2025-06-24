package uk.gov.justice.services.resources;

import java.util.Set;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.resources.rest.streams.StreamsResource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

class FrameworkApplicationTest {

    private final FrameworkApplication frameworkApplication = new FrameworkApplication();

    @Test
    void validateConfiguredResourceClasses() {
        final Set<Class<?>> resourceClasses = frameworkApplication.getClasses();

        assertThat(resourceClasses.size(), is(1));
        assertThat(resourceClasses, contains(StreamsResource.class));
    }
}