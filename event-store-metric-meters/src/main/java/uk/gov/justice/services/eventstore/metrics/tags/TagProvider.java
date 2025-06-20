package uk.gov.justice.services.eventstore.metrics.tags;

import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.ENV_TAG_NAME;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.SERVICE_TAG_NAME;

import uk.gov.justice.services.jdbc.persistence.JndiAppNameProvider;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.List;

import javax.inject.Inject;

import io.micrometer.core.instrument.Tag;

public class TagProvider {

    public record SourceComponentPair(String source, String component) {
    }

    @Inject
    private JndiAppNameProvider jndiAppNameProvider;

    @Inject
    private MetricsConfiguration metricsConfiguration;

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    public List<SourceComponentPair> getSourceComponentPairs() {

        return subscriptionsDescriptorsRegistry.getAll().stream()
                .filter(subscriptionsDescriptor -> !EVENT_PROCESSOR.equalsIgnoreCase(subscriptionsDescriptor.getServiceComponent()))
                .flatMap(subscriptionsDescriptor -> subscriptionsDescriptor.getSubscriptions().stream()
                        .map(subscription ->
                                new SourceComponentPair(subscription.getEventSourceName(), subscriptionsDescriptor.getServiceComponent())
                        ))
                .toList();
    }

    public List<Tag> getGlobalTags() {
        return List.of(
                Tag.of(SERVICE_TAG_NAME.getTagName(), jndiAppNameProvider.getAppName()),
                Tag.of(ENV_TAG_NAME.getTagName(), metricsConfiguration.micrometerEnv()));
    }

}