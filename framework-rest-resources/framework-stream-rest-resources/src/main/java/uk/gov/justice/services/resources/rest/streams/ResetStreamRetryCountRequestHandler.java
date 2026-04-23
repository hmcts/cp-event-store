package uk.gov.justice.services.resources.rest.streams;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository;
import uk.gov.justice.services.resources.rest.model.ResetStreamRetryCountResponse;

import java.util.UUID;

import jakarta.inject.Inject;

public class ResetStreamRetryCountRequestHandler {

    @Inject
    private StreamErrorRetryRepository streamErrorRetryRepository;

    @Inject
    private UtcClock clock;

    public ResetStreamRetryCountResponse doResetStreamRetryCount(final UUID streamId, final String source, final String component) {

        if (streamErrorRetryRepository.resetRetriesToZero(
                streamId,
                source,
                component,
                clock.now())) {

            return new ResetStreamRetryCountResponse(
                    true,
                    "Stream retry count reset to zero",
                    streamId,
                    source,
                    component
            );
        }

        return new ResetStreamRetryCountResponse(
                false,
                "Failed to reset stream retry count. No stream retry found for streamId: '%s', source: '%s', component: '%s'"
                        .formatted(streamId, source, component),
                streamId,
                source,
                component
        );
    }
}
