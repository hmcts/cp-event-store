package uk.gov.justice.services.eventsourcing.eventreader;

import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;

@RestReader
public class RestNextEventReader implements NextEventReader {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private EventStoreHttpClient eventStoreHttpClient;

    @Override
    public Optional<JsonEnvelope> read(final UUID streamId, final Long position, final String source) {

        final String baseRestUri = eventSourceDefinitionRegistry.getRestUri(source);

        return eventStoreHttpClient.getNextEvent(baseRestUri, streamId, position);
    }
}
