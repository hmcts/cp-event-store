package uk.gov.justice.services.eventsource;

import static java.lang.String.format;

import uk.gov.justice.services.common.configuration.ContextNameProvider;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventDestinationResolver;
import uk.gov.justice.services.eventsourcing.publisher.jms.JmsEventPublisher;
import uk.gov.justice.services.messaging.context.ContextName;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Provides Listener endpoint name to {@link JmsEventPublisher}.
 */
@ApplicationScoped
public class DefaultEventDestinationResolver implements EventDestinationResolver {

    // TODO: remove this when available in framework-api
    private static String ADMINISTRATION_NAMING_PREFIX = "administration";

    @Inject
    private ContextNameProvider contextNameProvider;

    @Override
    public String destinationNameOf(final String eventName) {
        return format("%s.event", getContextName(eventName));
    }

    private String getContextName(final String eventName) {

        final String contextNameFromEvent = ContextName.fromName(eventName);

        if (ADMINISTRATION_NAMING_PREFIX.equals(contextNameFromEvent)) {
            return contextNameProvider.getContextName();
        }

        return contextNameFromEvent;
    }
}
