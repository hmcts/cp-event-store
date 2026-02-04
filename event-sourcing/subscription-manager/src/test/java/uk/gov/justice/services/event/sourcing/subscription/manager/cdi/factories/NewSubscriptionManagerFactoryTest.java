package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManagerDelegate;
import uk.gov.justice.services.subscription.SubscriptionManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewSubscriptionManagerFactoryTest {

    @Mock
    private NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;

    @Mock
    private EventPullConfiguration eventPullConfiguration;

    @InjectMocks
    private NewSubscriptionManagerFactory newSubscriptionManagerFactory;

    @Test
    public void shouldCreateNewSubscriptionManagerForSpecifiedComponent() throws Exception {

        final String componentName = "some-component";

        final SubscriptionManager subscriptionManager = newSubscriptionManagerFactory.create(componentName);

        assertThat(subscriptionManager, is(instanceOf(NewSubscriptionManager.class)));

        assertThat(getValueOfField(subscriptionManager, "newSubscriptionManagerDelegate", NewSubscriptionManagerDelegate.class), is(newSubscriptionManagerDelegate));
        assertThat(getValueOfField(subscriptionManager, "eventPullConfiguration", EventPullConfiguration.class), is(eventPullConfiguration));
        assertThat(getValueOfField(subscriptionManager, "componentName", String.class), is(componentName));
    }
}