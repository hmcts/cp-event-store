package uk.gov.justice.services.resources;

import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import uk.gov.justice.services.resources.rest.CorsFilter;
import uk.gov.justice.services.resources.rest.streams.ActiveErrorsResource;
import uk.gov.justice.services.resources.rest.streams.StreamErrorsResource;
import uk.gov.justice.services.resources.rest.streams.StreamsResource;

@ApplicationPath("/internal")
public class FrameworkApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> classes = new HashSet<>();
        classes.add(StreamsResource.class);
        classes.add(StreamErrorsResource.class);
        classes.add(ActiveErrorsResource.class);
        classes.add(CorsFilter.class);
        return classes;
    }
}
