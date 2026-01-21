package uk.gov.justice.services.eventsourcing.eventpoller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventStoreEventDiscoveryException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

@ExtendWith(MockitoExtension.class)
class EventStreamPollerConfigTest {

    @InjectMocks
    private EventStreamPollerConfig eventPollerConfig;

    @Test
    void shouldGetBatchSizeAsInt() {

        final int batchSize = 100;
        setField(eventPollerConfig, "batchSize", batchSize + "");

        assertThat(eventPollerConfig.getBatchSize(), is(batchSize));
    }

    @Test
    void shouldThrowIfBatchSizeIsNotAnInteger() {

        setField(eventPollerConfig, "batchSize", "non-integer value");

        final EventStreamPollerConfigurationException eventStreamPollerConfigurationException = assertThrows(
                EventStreamPollerConfigurationException.class,
                () -> eventPollerConfig.getBatchSize());

        assertThat(eventStreamPollerConfigurationException.getMessage(), is("'event.poller.batch.size' jndi value is not an integer. Was 'non-integer value'"));
        assertThat(eventStreamPollerConfigurationException.getCause(), is(instanceOf(NumberFormatException.class)));
    }
}