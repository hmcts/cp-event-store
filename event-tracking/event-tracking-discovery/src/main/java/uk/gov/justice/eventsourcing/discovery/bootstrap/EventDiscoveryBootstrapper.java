package uk.gov.justice.eventsourcing.discovery.bootstrap;

import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import static java.lang.String.format;

public class EventDiscoveryBootstrapper {

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Inject
    private Logger logger;

    public void bootstrapEventDiscovery() {

        subscriptionSourceComponentFinder
                .findSourceComponentPairsFromSubscriptionRegistry()
                .forEach(this::doBootstrap);
    }

    private void doBootstrap(final SourceComponentPair sourceComponentPair) {

        final String source = sourceComponentPair.source();
        final String component = sourceComponentPair.component();
        final Optional<EventSubscriptionStatus> eventSubscriptionStatus = eventSubscriptionStatusRepository.findBy(source, component);

        if(eventSubscriptionStatus.isEmpty()) {
            eventSubscriptionStatusRepository.insertEmptyRowFor(
                    source,
                    component);

            logger.info(format("Inserted empty row into event_subscription_status for source '%s', component '%s'", source, component));
        }
    }
}
