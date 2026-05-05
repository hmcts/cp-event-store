package uk.gov.justice.services.eventsourcing.source.api.resource;

import java.util.HashSet;
import java.util.Set;

import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;

@ApplicationPath("/rest")
public class EventSourceApiApplication extends Application {


    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<>();
        classes.add(EventPageResource.class);
        classes.add(EventStreamPageResource.class);
        classes.add(EventDiscoveryResource.class);
        classes.add(EventResource.class);
        return classes;
    }
}
