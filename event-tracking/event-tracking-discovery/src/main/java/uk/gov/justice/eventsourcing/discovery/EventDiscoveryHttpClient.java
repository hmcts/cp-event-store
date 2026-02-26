package uk.gov.justice.eventsourcing.discovery;

import uk.gov.justice.services.clients.core.HttpCallerResponse;
import uk.gov.justice.services.clients.core.httpclient.DefaultHttpCaller;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventStoreEventDiscoveryException;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@ApplicationScoped
public class EventDiscoveryHttpClient {

    @Inject
    private DefaultHttpCaller defaultHttpCaller;

    @Inject
    private ObjectMapper objectMapper;

    public DiscoveryResult discoverEvents(final String restUri, final Optional<UUID> latestKnownEventId, final int batchSize) {

        final String url = buildUrl(restUri, latestKnownEventId, batchSize);
        final HttpCallerResponse response = defaultHttpCaller.get(url, Map.of("Accept", "application/json"));

        if (response.getStatusCode() != 200) {
            throw new EventStoreEventDiscoveryException(
                    "Failed to discover events from event store. Status: " + response.getStatusCode() +
                    ", restUri: " + restUri + ", latestKnownEventId: " + latestKnownEventId + ", batchSize: " + batchSize);
        }

        try {
            return objectMapper.readValue(response.getBody(), DiscoveryResult.class);
        } catch (final JsonProcessingException e) {
            throw new EventStoreEventDiscoveryException(
                    "Failed to parse discovery result: " + e.getMessage() +
                    ", restUri: " + restUri + ", latestKnownEventId: " + latestKnownEventId + ", batchSize: " + batchSize, e);
        }
    }

    private String buildUrl(final String restUri, final Optional<UUID> latestKnownEventId, final int batchSize) {
        final StringBuilder url = new StringBuilder(restUri)
                .append("/events-discovery?batchSize=").append(batchSize);
        latestKnownEventId.ifPresent(id -> url.append("&afterEventId=").append(id));
        return url.toString();
    }
}
