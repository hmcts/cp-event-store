package uk.gov.justice.services.healthcheck.healthchecks.artemis;

import java.util.List;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

public class DefaultDestinationNamesProvider implements DestinationNamesProvider {

    private static final List<String> DESTINATION_NAME_PATTERNS = List.of("%s.controller.command", "%s.handler.command", "%s.event");

    @Inject
    private JndiContextNameProvider jndiContextNameProvider;

    public List<String> getDestinationNames() {
        return DESTINATION_NAME_PATTERNS.stream()
                .map(qp -> String.format(qp, getContextName()))
                .collect(Collectors.toList());
    }

    protected final String getContextName() {
        return jndiContextNameProvider.getContextName();
    }
}
