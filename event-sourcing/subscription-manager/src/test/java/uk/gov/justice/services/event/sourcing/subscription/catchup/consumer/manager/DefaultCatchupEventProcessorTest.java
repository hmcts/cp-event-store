package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.event.sourcing.subscription.manager.CatchupEventBufferProcessor;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultCatchupEventProcessorTest {

    @Mock
    private CatchupEventBufferProcessor catchupEventBufferProcessor;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private DefaultTransactionalEventProcessor defaultTransactionalEventProcessor;

    @Test
    public void shouldProcessWithEventBufferAndAlwaysReturnOne() throws Exception {

        final String subscriptionName = "subscriptionName";
        final PublishedEvent publishedEvent = mock(PublishedEvent.class);
        final JsonEnvelope eventEnvelope = mock(JsonEnvelope.class);

        when(eventConverter.envelopeOf(publishedEvent)).thenReturn(eventEnvelope);

        assertThat(defaultTransactionalEventProcessor.processWithEventBuffer(publishedEvent, subscriptionName), is(1));

        verify(catchupEventBufferProcessor).processWithEventBuffer(eventEnvelope, subscriptionName);
    }
}
