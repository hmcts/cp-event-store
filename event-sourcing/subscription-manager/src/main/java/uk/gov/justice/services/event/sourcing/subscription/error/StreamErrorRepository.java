package uk.gov.justice.services.event.sourcing.subscription.error;

import static javax.transaction.Transactional.TxType.MANDATORY;
import static javax.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

@SuppressWarnings("java:S1192")
public class StreamErrorRepository {

    @Inject
    private StreamErrorPersistence streamErrorPersistence;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Inject
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;

    @Inject
    private Logger logger;

    @Transactional(MANDATORY)
    public void markStreamAsErrored(final StreamError streamError, final Long expectedPositionInStream) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            final StreamErrorDetails streamErrorDetails = streamError.streamErrorDetails();
            final UUID streamId = streamErrorDetails.streamId();
            final UUID streamErrorId = streamErrorDetails.id();
            final Long errorPositionInStream = streamErrorDetails.positionInStream();
            final String componentName = streamErrorDetails.componentName();
            final String source = streamErrorDetails.source();

            final Long currentPositionInStream = streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, componentName, connection);

            if (checkStreamStatusPositionNotChanged(expectedPositionInStream, currentPositionInStream)) {
                if (streamErrorPersistence.save(streamError, connection)) {
                    streamStatusErrorPersistence.markStreamAsErrored(
                            streamId,
                            streamErrorId,
                            errorPositionInStream,
                            componentName,
                            source,
                            connection);
                }
            } else
                logger.warn("Stream Status Position is changed after last Error, cannot update stream status." +
                            " streamId: {} component: {} source: {} expected positio.n: {} actual position: {}",
                        streamId, componentName, source, expectedPositionInStream, currentPositionInStream);

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }

    private static boolean checkStreamStatusPositionNotChanged(final Long expectedPositionInStream, final Long currentPositionInStream) {
        return Objects.equals(expectedPositionInStream, currentPositionInStream);
    }

    @Transactional(MANDATORY)
    public void markStreamAsFixed(final UUID streamErrorId, final UUID streamId, final String source, final String componentName) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            streamStatusErrorPersistence.unmarkStreamStatusAsErrored(streamId, source, componentName, connection);
            streamErrorPersistence.removeErrorForStream(streamErrorId, streamId, source, componentName, connection);
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }

    @Transactional(REQUIRED)
    public List<StreamError> findAllByStreamId(final UUID streamId) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            return streamErrorPersistence.findAllByStreamId(streamId, connection);
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }

    @Transactional(REQUIRED)
    public Optional<StreamError> findByErrorId(final UUID errorId) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            return streamErrorPersistence.findByErrorId(errorId, connection);
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }

    @Transactional(REQUIRED)
    public void markSameErrorHappened(final StreamError newStreamError, final long lastStreamPosition, final Timestamp lastUpdatedAt) {
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            final int numberOfChange = streamStatusErrorPersistence.updateStreamStatusUpdatedAtForSameError(newStreamError, lastStreamPosition, lastUpdatedAt, connection);
            if (numberOfChange == 0) {
                logger.warn("Existing stream status entry is changed by another transaction errorId: {} streamId: {} source {} component {}",
                        newStreamError.streamErrorDetails().id(),
                        newStreamError.streamErrorDetails().streamId(),
                        newStreamError.streamErrorDetails().source(),
                        newStreamError.streamErrorDetails().componentName());
            }
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }
}
