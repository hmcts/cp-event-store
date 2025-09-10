package uk.gov.justice.services.eventstore.metrics.tags;

import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;
import static uk.gov.justice.services.metrics.micrometer.config.TagNames.ENV_TAG_NAME;
import static uk.gov.justice.services.metrics.micrometer.config.TagNames.SERVICE_TAG_NAME;

import uk.gov.justice.services.common.configuration.ContextNameProvider;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;
import uk.gov.justice.services.metrics.micrometer.meters.SourceComponentPair;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.List;

import javax.inject.Inject;

import io.micrometer.core.instrument.Tag;

public class TagProvider {

    @Inject
    private ContextNameProvider contextNameProvider;

    @Inject
    private MetricsConfiguration metricsConfiguration;

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    public List<SourceComponentPair> getSourceComponentPairs() {

        return subscriptionsDescriptorsRegistry.getAll().stream()
                .filter(subscriptionsDescriptor -> !EVENT_PROCESSOR.equalsIgnoreCase(subscriptionsDescriptor.getServiceComponent()))
                .flatMap(subscriptionsDescriptor -> subscriptionsDescriptor.getSubscriptions().stream()
                        .map(subscription ->
                                new SourceComponentPair(eventSourceNameCalculator.getSource(subscription.getEventSourceName()), subscriptionsDescriptor.getServiceComponent())
                        ))
                .toList();
    }

    public List<Tag> getCommonTags() {
        return List.of(
                Tag.of(SERVICE_TAG_NAME.getTagName(), contextNameProvider.getContextName()),
                Tag.of(ENV_TAG_NAME.getTagName(), metricsConfiguration.micrometerEnv()));
    }

}