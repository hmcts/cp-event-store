package uk.gov.justice.services.eventstore.management.replay.process;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;
import static uk.gov.justice.subscription.domain.builders.SubscriptionBuilder.subscription;
import static uk.gov.justice.subscription.domain.builders.SubscriptionsDescriptorBuilder.subscriptionsDescriptor;

import uk.gov.justice.services.eventstore.management.catchup.process.PriorityComparatorProvider;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSourceNameFinderTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    private PriorityComparatorProvider priorityComparatorProvider;

    private EventSourceNameFinder eventSourceNameFinder;

    @BeforeEach
    public void setup() {
        priorityComparatorProvider = new PriorityComparatorProvider();
        eventSourceNameFinder = new EventSourceNameFinder(subscriptionsDescriptorsRegistry, priorityComparatorProvider);
    }

    @Test
    public void shouldFilterEventListenerSubscriptionDescriptorAndFetchEventSourceNameFromSortedSubscriptions() {
        final SubscriptionsDescriptor eventListenerSD = subscriptionsDescriptor()
                .withServiceComponent(EVENT_LISTENER)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName("listenerEventSourceName2")
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName("listenerEventSourceName1")
                                .build()
                ))
                .build();
        final SubscriptionsDescriptor eventProcessorSD = subscriptionsDescriptor()
                .withServiceComponent(EVENT_PROCESSOR)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName("processorEventSourceName2")
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName("processorEventSourceName1")
                                .build()
                ))
                .build();
        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(eventListenerSD, eventProcessorSD));

        final String eventSourceName = eventSourceNameFinder.getEventSourceNameOf(EVENT_LISTENER);

        assertThat(eventSourceName, is("listenerEventSourceName1"));
    }

    @Test
    public void shouldThrowExceptionWhenSubscriptionsNotFoundForEventListenerSubscriptionDescriptor() {
        final SubscriptionsDescriptor eventListenerSD = subscriptionsDescriptor()
                .withServiceComponent(EVENT_LISTENER)
                .withSubscriptions(emptyList())
                .build();
        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(eventListenerSD));

        final ReplayEventFailedException e = assertThrows(ReplayEventFailedException.class, () -> eventSourceNameFinder.getEventSourceNameOf(EVENT_LISTENER));

        assertThat(e.getMessage(), is("No event source name found for event listener"));
    }

    @Test
    public void shouldThrowExceptionWhenEventListenerSubscriptionDescriptorNotFound() {
        final SubscriptionsDescriptor eventProcessorSD = subscriptionsDescriptor()
                .withServiceComponent(EVENT_PROCESSOR)
                .withSubscriptions(singletonList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName("processorEventSourceName2")
                                .build()
                ))
                .build();
        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(eventProcessorSD));

        final ReplayEventFailedException e = assertThrows(ReplayEventFailedException.class, () -> eventSourceNameFinder.getEventSourceNameOf(EVENT_LISTENER));

        assertThat(e.getMessage(), is("No event source name found for event listener"));
    }

    @Test
    public void shouldEnsureEventSourceNameExistsInRegistry() throws Exception {

        final String listenerEventSourceName_1 = "listenerEventSourceName_1";
        final String listenerEventSourceName_2 = "listenerEventSourceName_2";
        final String processorEventSourceName_1 = "processorEventSourceName_1";
        final String processorEventSourceName_2 = "processorEventSourceName_2";

        final SubscriptionsDescriptor eventProcessorSubscriptionsDescriptor = subscriptionsDescriptor()
                .withServiceComponent(EVENT_PROCESSOR)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName(processorEventSourceName_2)
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName(processorEventSourceName_1)
                                .build()
                ))
                .build();
        final SubscriptionsDescriptor eventListenerSubscriptionsDescriptor = subscriptionsDescriptor()
                .withServiceComponent(EVENT_LISTENER)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName(listenerEventSourceName_2)
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName(listenerEventSourceName_1)
                                .build()
                ))
                .build();
        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(eventListenerSubscriptionsDescriptor, eventProcessorSubscriptionsDescriptor));

        assertThat(eventSourceNameFinder.ensureEventSourceNameExistsInRegistry(listenerEventSourceName_2, EVENT_LISTENER), is(listenerEventSourceName_2));
    }

    @Test
    public void shouldThrowReplayEventFailedExceptionIfEventSourceNameNotFoundInRegistry() throws Exception {

        final String wrongEventSourceName = "something-silly";

        final SubscriptionsDescriptor eventProcessorSubscriptionsDescriptor = subscriptionsDescriptor()
                .withServiceComponent(EVENT_PROCESSOR)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName("processorEventSourceName_2")
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName("processorEventSourceName_1")
                                .build()
                ))
                .build();
        final SubscriptionsDescriptor eventListenerSubscriptionsDescriptor = subscriptionsDescriptor()
                .withServiceComponent(EVENT_LISTENER)
                .withSubscriptions(asList(
                        subscription()
                                .withPrioritisation(2)
                                .withEventSourceName("listenerEventSourceName_2")
                                .build(),
                        subscription()
                                .withPrioritisation(1)
                                .withEventSourceName("listenerEventSourceName_1")
                                .build()
                ))
                .build();
        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(eventListenerSubscriptionsDescriptor, eventProcessorSubscriptionsDescriptor));

        final ReplayEventFailedException replayEventFailedException = assertThrows(ReplayEventFailedException.class,
                () -> eventSourceNameFinder.ensureEventSourceNameExistsInRegistry(
                        wrongEventSourceName,
                        EVENT_LISTENER));

        assertThat(replayEventFailedException.getMessage(), is("No event source named 'something-silly' found in subscriptions-descriptor.yaml file(s) for component 'EVENT_LISTENER'"));
    }
}