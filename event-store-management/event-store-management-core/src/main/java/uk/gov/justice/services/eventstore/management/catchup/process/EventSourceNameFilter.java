package uk.gov.justice.services.eventstore.management.catchup.process;

import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;

import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;

public class EventSourceNameFilter {

    @Inject
    private CatchupWhitelistedEventSourcesProvider catchupWhitelistedEventSourcesProvider;

    /**
     * Will return true if either the event-source name is found in our JNDI list of whitelisted event sources
     * or if the JNDI value has not been set
     *
     * @param subscription The Subscription that contains the name of an event source used for catchup
     * @return true if the event-source name is found in our JNDI list of whitelisted event sources
     */
    public boolean shouldRunCatchupAgainstEventSource(final Subscription subscription) {

        final Optional<List<String>> whiteList = catchupWhitelistedEventSourcesProvider.getWhiteListedEventSources();

        return whiteList.map(
                whitelistedEventSources -> whitelistedEventSources.contains(subscription.getEventSourceName()))
                .orElse(true);
    }
}
