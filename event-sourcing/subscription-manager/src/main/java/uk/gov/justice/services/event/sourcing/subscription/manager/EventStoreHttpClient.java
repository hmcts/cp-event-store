package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.clients.core.HttpCallerResponse;
import uk.gov.justice.services.clients.core.httpclient.DefaultHttpCaller;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@ApplicationScoped
public class EventStoreHttpClient {

    @Inject
    private DefaultHttpCaller defaultHttpCaller;

    @Inject
    private LinkedEventMapper linkedEventMapper;

    public Optional<JsonEnvelope> getNextEvent(final String restUri, final UUID streamId, final Long afterPosition) {

        final String url = restUri + "/event/" + streamId + "?afterPosition=" + afterPosition;

        final HttpCallerResponse response = defaultHttpCaller.get(url, Map.of("Accept", "application/json"));

        if (response.getStatusCode() == 204) {
            return empty();
        }

        if (response.getStatusCode() != 200) {
            throw new RestNextEventReaderException(
                    "Failed to read next event from event store. Status: " + response.getStatusCode() +
                    ", streamId: " + streamId + ", afterPosition: " + afterPosition);
        }

        return of(linkedEventMapper.toJsonEnvelope(response.getBody()));
    }
}
