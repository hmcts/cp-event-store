package uk.gov.justice.services.eventsourcing.util.messaging;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSourceNameCalculatorTest {

    @InjectMocks
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Test
    public void shouldParseTheEventNameToGetTheSourceContext() throws Exception {

        final String eventName = "source-context.event.name";

        final JsonEnvelope event = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(event.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);

        assertThat(eventSourceNameCalculator.getSource(event), is("source-context"));
        assertThat(eventSourceNameCalculator.getSource("source-context.event.name"), is("source-context"));
    }

    @Test
    public void shouldReturnWholeNameIfNoDotPresent() throws Exception {
        final String eventName = "source";
        assertThat(eventSourceNameCalculator.getSource(eventName), is("source"));
    }

}
