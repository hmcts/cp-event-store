package uk.gov.justice.services.eventstore.management.catchup.state;

import uk.gov.justice.services.eventstore.management.catchup.process.CatchupInProgress;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CatchupStateManager {

    private final Map<SubscriptionCatchupDetails, CatchupInProgress> catchupsInProgress = new ConcurrentHashMap<>();

    public void clear() {
        catchupsInProgress.clear();
    }

    public void newCatchupInProgress(
            final List<SubscriptionCatchupDetails> subscriptionCatchupDetailsList,
            final ZonedDateTime catchupStartedAt) {

        subscriptionCatchupDetailsList.forEach(subscriptionCatchupDetails ->
            catchupsInProgress.put(
                    subscriptionCatchupDetails,
                    new CatchupInProgress(subscriptionCatchupDetails, catchupStartedAt))
        );
    }

    public CatchupInProgress removeCatchupInProgress(final SubscriptionCatchupDetails subscriptionCatchupDefinition) {
        return catchupsInProgress.remove(subscriptionCatchupDefinition);
    }

    public boolean isCatchupInProgress(final SubscriptionCatchupDetails subscriptionCatchupDefinition) {
        return catchupsInProgress.containsKey(subscriptionCatchupDefinition);
    }

    public boolean noCatchupsInProgress() {
        return catchupsInProgress.isEmpty();
    }

}
