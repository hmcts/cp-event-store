package uk.gov.justice.subscription;

import static java.util.stream.Collectors.toSet;

import uk.gov.justice.subscription.domain.subscriptiondescriptor.Event;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

@Singleton
@Startup
public class SubscriptionEventNamesProvider {

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    private Map<SourceComponentPair, Set<String>> eventNameMap;

    @PostConstruct
    public void init() {
        eventNameMap = buildEventNameMap();
    }

    public boolean accepts(final String eventName, final String source, final String component) {
        return getAcceptedEventNames(source, component).contains(eventName);
    }

    private Set<String> getAcceptedEventNames(final String source, final String component) {
        return eventNameMap.getOrDefault(new SourceComponentPair(source, component), Set.of());
    }

    private Map<SourceComponentPair, Set<String>> buildEventNameMap() {
        final Map<SourceComponentPair, Set<String>> eventNameMap = new HashMap<>();

        final List<SubscriptionsDescriptor> descriptors = subscriptionsDescriptorsRegistry.getAll();
        for (final SubscriptionsDescriptor descriptor : descriptors) {
            final String component = descriptor.getServiceComponent();

            for (final Subscription subscription : descriptor.getSubscriptions()) {
                final String source = subscription.getEventSourceName();
                final SourceComponentPair key = new SourceComponentPair(source, component);

                final Set<String> eventNames = subscription.getEvents().stream()
                        .map(Event::getName)
                        .collect(toSet());

                eventNameMap.put(key, eventNames);
            }
        }

        return eventNameMap;
    }
}
