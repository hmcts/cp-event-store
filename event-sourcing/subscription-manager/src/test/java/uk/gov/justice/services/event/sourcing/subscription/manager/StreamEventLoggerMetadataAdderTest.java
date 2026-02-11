package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.log.LoggerConstants.REQUEST_DATA;

import uk.gov.justice.services.common.configuration.ServiceContextNameProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.logging.TraceLogger;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamEventLoggerMetadataAdderTest {

    @Mock
    private Logger logger;

    @Mock
    private ServiceContextNameProvider serviceContextNameProvider;

    @Mock
    private TraceLogger traceLogger;

    @Mock
    private MdcWrapper mdcWrapper;

    @Captor
    private ArgumentCaptor<String> mdcValueCaptor;

    @InjectMocks
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Test
    public void shouldAddRequestDataToMdc() {

        final String componentName = "EVENT_LISTENER";
        final String serviceContextName = "usersgroups";

        final JsonObject metadataJsonObject = Json.createObjectBuilder()
                .add("id", "test-id")
                .add("name", "example.event-added")
                .build();

        final Metadata metadata = mock(Metadata.class);
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(serviceContextNameProvider.getServiceContextName()).thenReturn(serviceContextName);
        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.asJsonObject()).thenReturn(metadataJsonObject);

        streamEventLoggerMetadataAdder.addRequestDataToMdc(jsonEnvelope, componentName);

        verify(mdcWrapper).put(eq(REQUEST_DATA), mdcValueCaptor.capture());

        final String mdcValue = mdcValueCaptor.getValue();
        assertThat(mdcValue, containsString("\"serviceContext\":\"usersgroups\""));
        assertThat(mdcValue, containsString("\"serviceComponent\":\"EVENT_LISTENER\""));
        assertThat(mdcValue, containsString("\"metadata\":{"));
        assertThat(mdcValue, containsString("\"name\":\"example.event-added\""));
    }

    @Test
    public void shouldAddRequestDataToMdcWithoutServiceContextWhenNotAvailable() {

        final String componentName = "EVENT_LISTENER";

        final JsonObject metadataJsonObject = Json.createObjectBuilder()
                .add("id", "test-id")
                .build();

        final Metadata metadata = mock(Metadata.class);
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(serviceContextNameProvider.getServiceContextName()).thenReturn(null);
        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.asJsonObject()).thenReturn(metadataJsonObject);

        streamEventLoggerMetadataAdder.addRequestDataToMdc(jsonEnvelope, componentName);

        verify(mdcWrapper).put(eq(REQUEST_DATA), mdcValueCaptor.capture());

        final String mdcValue = mdcValueCaptor.getValue();
        assertThat(mdcValue, containsString("\"serviceComponent\":\"EVENT_LISTENER\""));
        assertThat(mdcValue, containsString("\"metadata\":{"));
    }

    @Test
    public void shouldHandleMetadataExtractionFailure() {

        final String componentName = "EVENT_LISTENER";

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(serviceContextNameProvider.getServiceContextName()).thenReturn("usersgroups");
        when(jsonEnvelope.metadata()).thenThrow(new RuntimeException("metadata error"));

        streamEventLoggerMetadataAdder.addRequestDataToMdc(jsonEnvelope, componentName);

        verify(mdcWrapper).put(eq(REQUEST_DATA), mdcValueCaptor.capture());

        final String mdcValue = mdcValueCaptor.getValue();
        assertThat(mdcValue, containsString("\"metadata\":\"Could not find: _metadata in envelope\""));
    }

    @Test
    public void shouldClearMdc() {

        streamEventLoggerMetadataAdder.clearMdc();

        verify(mdcWrapper).clear();
    }
}
