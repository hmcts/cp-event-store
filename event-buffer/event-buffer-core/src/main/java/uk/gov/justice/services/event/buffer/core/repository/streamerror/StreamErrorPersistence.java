package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class StreamErrorPersistence {

    @Inject
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @Inject
    private StreamErrorOccurrencePersistence streamErrorOccurrencePersistence;

    public boolean save(final StreamError streamError, final Connection connection) {

        try {
            streamErrorHashPersistence.upsert(streamError.streamErrorHash(), connection);
            final int rowsUpdated = streamErrorOccurrencePersistence.insert(streamError.streamErrorOccurrence(), connection);
            return rowsUpdated > 0;
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format("Failed to save StreamError: %s", streamError), e);
        }
    }

    public Optional<StreamError> findByErrorId(final UUID streamErrorId, final Connection connection) {
        try {
            final Optional<StreamErrorOccurrence> streamErrorDetailsOptional = streamErrorOccurrencePersistence.findById(streamErrorId, connection);

            if (streamErrorDetailsOptional.isPresent()) {
                final StreamErrorOccurrence streamErrorOccurrence = streamErrorDetailsOptional.get();
                final Optional<StreamErrorHash> streamErrorHashOptional = streamErrorHashPersistence.findByHash(streamErrorOccurrence.hash(), connection);
                if (streamErrorHashOptional.isPresent()) {
                    final StreamError streamError = new StreamError(streamErrorOccurrence, streamErrorHashOptional.get());
                    return of(streamError);
                }
            }

            return empty();

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format("Failed find StreamError by streamErrorId: '%s'", streamErrorId), e);
        }
    }

    public List<StreamError> findAllByStreamId(final UUID streamId, final Connection connection) {
        try {
            final List<StreamError> streamErrors = new ArrayList<>();
            final List<StreamErrorOccurrence> streamErrorOccurrenceList = streamErrorOccurrencePersistence.findByStreamId(streamId, connection);
            for (final StreamErrorOccurrence streamErrorOccurrence : streamErrorOccurrenceList) {
                final Optional<StreamErrorHash> streamErrorHashOptional = streamErrorHashPersistence.findByHash(streamErrorOccurrence.hash(), connection);
                if (streamErrorHashOptional.isPresent()) {
                    streamErrors.add(new StreamError(streamErrorOccurrence, streamErrorHashOptional.get()));
                } else {
                    throw new StreamErrorHandlingException("No stream_error found for hash '" + streamErrorOccurrence.hash() + "' yet hash exists in stream_error table");
                }
            }

            return streamErrors;

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format("Failed find List of StreamErrors by streamId: '%s'", streamId), e);
        }
    }

    public void removeErrorForStream(final UUID streamErrorId, final UUID streamId, final String source, final String componentName, final Connection connection) {

        try {
            final String hash = streamErrorOccurrencePersistence.deleteErrorAndGetHash(streamErrorId, connection);
            if (streamErrorOccurrencePersistence.noErrorsExistFor(hash, connection)) {
                streamErrorHashPersistence.deleteHash(hash, connection);
            }
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format(
                    "Failed to remove error for stream. streamId: '%s', source: '%s, component: '%s'",
                    streamId,
                    source,
                    componentName
            ), e);
        }
    }
}
