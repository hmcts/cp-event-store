package uk.gov.justice.services.resources.rest.streams;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.resources.rest.model.ErrorResponse;

import java.util.UUID;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class EventsDiscoveryResourceTest {

    @Mock
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventsDiscoveryResource eventsDiscoveryResource;

    @Nested
    class SuccessScenarioTest {

        @Test
        void shouldReturnDiscoveryResultAsJson() throws Exception {
            final UUID afterEventId = randomUUID();
            final int limit = 100;
            final UUID latestEventId = randomUUID();
            final DiscoveryResult discoveryResult = new DiscoveryResult(emptyList(), of(latestEventId));

            when(eventSubscriptionDiscoveryBean.discoverNewEvents(ofNullable(afterEventId), limit)).thenReturn(discoveryResult);
            when(objectMapper.writeValueAsString(discoveryResult)).thenReturn("{\"streamPositions\":[],\"latestKnownEventId\":\"" + latestEventId + "\"}");

            try (Response response = eventsDiscoveryResource.discoverEvents(afterEventId, limit)) {
                assertThat(response.getStatus(), is(200));
                assertThat(response.getHeaderString("Content-type"), is("application/json"));
                final String json = (String) response.getEntity();
                assertThat(json, is("{\"streamPositions\":[],\"latestKnownEventId\":\"" + latestEventId + "\"}"));
            }
        }

        @Test
        void shouldReturnDiscoveryResultWithNullAfterEventId() throws Exception {
            final int limit = 100;
            final DiscoveryResult discoveryResult = new DiscoveryResult(emptyList(), of(randomUUID()));

            when(eventSubscriptionDiscoveryBean.discoverNewEvents(empty(), limit)).thenReturn(discoveryResult);
            when(objectMapper.writeValueAsString(discoveryResult)).thenReturn("{}");

            try (Response response = eventsDiscoveryResource.discoverEvents(null, limit)) {
                assertThat(response.getStatus(), is(200));
                assertThat(response.getHeaderString("Content-type"), is("application/json"));
            }
        }
    }

    @Nested
    class ErrorScenarioTest {

        @Test
        void shouldReturnInternalServerErrorOnException() {
            final UUID afterEventId = randomUUID();
            final int limit = 100;

            doThrow(new RuntimeException("Database error")).when(eventSubscriptionDiscoveryBean).discoverNewEvents(ofNullable(afterEventId), limit);

            try (Response response = eventsDiscoveryResource.discoverEvents(afterEventId, limit)) {
                assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
                assertTrue(response.getEntity() instanceof ErrorResponse);
                assertThat(((ErrorResponse) response.getEntity()).errorMessage(), is("An error occurred while processing the request"));
            }
        }
    }
}
