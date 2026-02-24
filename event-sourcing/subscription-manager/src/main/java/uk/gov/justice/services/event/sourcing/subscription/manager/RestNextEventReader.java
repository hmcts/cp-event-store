package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

@RestReader
public class RestNextEventReader implements NextEventReader {

    @Override
    public Optional<JsonEnvelope> read(final UUID streamId, final Long position) {
        throw new UnsupportedOperationException("RestNextEventReader is not yet implemented");
    }
}
