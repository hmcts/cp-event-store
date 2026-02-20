package uk.gov.justice.services.eventsourcing.discovery;

import static java.util.Optional.empty;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RestEventSubscriptionDiscovererTest {

    @InjectMocks
    private RestEventSubscriptionDiscoverer restEventSubscriptionDiscoverer;

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhenDiscoverNewEventsIsCalled() {

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> restEventSubscriptionDiscoverer.discoverNewEvents(empty()));

        assertThat(exception.getMessage(), is("RestEventSubscriptionDiscoverer is not yet implemented"));
    }
}
