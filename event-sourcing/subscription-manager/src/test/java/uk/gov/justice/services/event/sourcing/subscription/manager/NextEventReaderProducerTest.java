package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NextEventReaderProducerTest {

    @Mock
    private NextEventReader transactionalNextEventReader;

    @Mock
    private NextEventReader restNextEventReader;

    @Mock
    private StreamProcessingConfig streamProcessingConfig;

    @InjectMocks
    private NextEventReaderProducer nextEventReaderProducer;

    @Test
    public void shouldReturnTransactionalNextEventReaderWhenAccessEventStoreViaRestIsFalse() {

        when(streamProcessingConfig.accessEventStoreViaRest()).thenReturn(false);

        assertThat(nextEventReaderProducer.nextEventReader(), is(transactionalNextEventReader));
    }

    @Test
    public void shouldReturnRestNextEventReaderWhenAccessEventStoreViaRestIsTrue() {

        when(streamProcessingConfig.accessEventStoreViaRest()).thenReturn(true);

        assertThat(nextEventReaderProducer.nextEventReader(), is(restNextEventReader));
    }
}
