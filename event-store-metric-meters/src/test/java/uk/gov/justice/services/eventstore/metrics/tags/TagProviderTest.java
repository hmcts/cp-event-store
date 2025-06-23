package uk.gov.justice.services.eventstore.metrics.tags;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;
import static uk.gov.justice.services.metrics.micrometer.config.TagNames.ENV_TAG_NAME;
import static uk.gov.justice.services.metrics.micrometer.config.TagNames.SERVICE_TAG_NAME;

import uk.gov.justice.services.eventstore.metrics.tags.TagProvider.SourceComponentPair;
import uk.gov.justice.services.jdbc.persistence.JndiAppNameProvider;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Event;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.List;

import io.micrometer.core.instrument.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TagProviderTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Mock
    private JndiAppNameProvider jndiAppNameProvider;

    @Mock
    private MetricsConfiguration metricsConfiguration;

    @InjectMocks
    private TagProvider tagProvider;

    @Test
    public void shouldGetSourceComponentPairsAndFilterOutEventProcessorComponents() throws Exception {

        List<Event> eventList = List.of(new Event("test-event", "test-schema-uri"));
        final SubscriptionsDescriptor subscriptionsDescriptor1 = new SubscriptionsDescriptor(
                "1.0", "service-1", "some-component-1", 1, List.of(
                        new Subscription("subscription-1-1", eventList, "source-1-1", 1),
                        new Subscription("subscription-1-2", eventList, "source-1-2", 1)
                ));

        final SubscriptionsDescriptor subscriptionsDescriptor2 = new SubscriptionsDescriptor(
                "1.0", "service-3", EVENT_PROCESSOR, 1, List.of(
                        new Subscription("subscription-3-1", eventList, "source-3-1", 1)
                ));

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(List.of(subscriptionsDescriptor1, subscriptionsDescriptor2));

        final List<SourceComponentPair> sourceComponentPairs = tagProvider.getSourceComponentPairs();

        // Verify that EVENT_PROCESSOR component is filtered out
        assertThat(sourceComponentPairs.size(), is(2));

        assertThat(sourceComponentPairs.get(0), is(new SourceComponentPair("source-1-1", "some-component-1")));
        assertThat(sourceComponentPairs.get(1), is(new SourceComponentPair("source-1-2", "some-component-1")));

    }

    @Test
    public void shouldGetGlobalTags() {
        // Given
        final String appName = "test-app";
        final String env = "test-env";

        when(jndiAppNameProvider.getAppName()).thenReturn(appName);
        when(metricsConfiguration.micrometerEnv()).thenReturn(env);

        // When
        final List<Tag> globalTags = tagProvider.getGlobalTags();

        // Then
        assertThat(globalTags.size(), is(2));
        assertThat(globalTags.get(0), is(Tag.of(SERVICE_TAG_NAME.getTagName(), appName)));
        assertThat(globalTags.get(1), is(Tag.of(ENV_TAG_NAME.getTagName(), env)));
    }
}