package uk.gov.justice.services.eventsourcing.publisher.jms;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.jms.JmsEnvelopeSender;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class JmsEventPublisherTest {

    private static final String DESTINATION_NAME = "destinationName";

    private static final String EVENT_NAME = "test.event.listener";

    @Mock
    private Logger logger;

    @Mock
    private JmsEnvelopeSender jmsEnvelopeSender;

    @Mock
    private EventDestinationResolver eventDestinationResolver;

    @InjectMocks
    private JmsEventPublisher jmsEventPublisher;

    @Test
    public void shouldPublishEnvelope() {

        final JsonEnvelope envelope = JsonEnvelope.envelopeFrom(
                metadataBuilder()
                        .withName(EVENT_NAME)
                        .withId(randomUUID()),
                getJsonBuilderFactory().createObjectBuilder().build()
        );

        when(eventDestinationResolver.destinationNameOf(EVENT_NAME)).thenReturn(DESTINATION_NAME);

        jmsEventPublisher.publish(envelope);

        verify(jmsEnvelopeSender).send(envelope, DESTINATION_NAME);
        verify(logger).trace("Publishing event {} to {}", EVENT_NAME, DESTINATION_NAME);
    }

}
