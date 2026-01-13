package uk.gov.justice.services.eventsourcing.discovery;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventStoreEventDiscoveryException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryConfigTest {

    @InjectMocks
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Test
    public void shouldGetBatchSizeAsInt() throws Exception {

        final int batchSize = 23;
        setField(eventDiscoveryConfig, "batchSize", batchSize + "");

        assertThat(eventDiscoveryConfig.getBatchSize(), is(batchSize));
    }

    @Test
    public void shouldThrowIfBatchSizeIsNotAnInteger() throws Exception {

        setField(eventDiscoveryConfig, "batchSize", "something-silly");

        final EventStoreEventDiscoveryException eventStoreEventDiscoveryException = assertThrows(
                EventStoreEventDiscoveryException.class,
                () -> eventDiscoveryConfig.getBatchSize());

        assertThat(eventStoreEventDiscoveryException.getMessage(), is("'event.discovery.batch.size' jndi value is not an integer. Was 'something-silly'"));
        assertThat(eventStoreEventDiscoveryException.getCause(), is(instanceOf(NumberFormatException.class)));
    }
}