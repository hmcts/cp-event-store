package uk.gov.justice.services.event.sourcing.subscription.error;

import static javax.transaction.Transactional.TxType.MANDATORY;
import static javax.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;
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
    private Logger logger;

    @Transactional(MANDATORY)
    public void markStreamAsErrored(final StreamError streamError, final Long expectedPositionInStream) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            final StreamErrorOccurrence streamErrorOccurrence = streamError.streamErrorOccurrence();
            final UUID streamId = streamErrorOccurrence.streamId();
            final UUID streamErrorId = streamErrorOccurrence.id();
            final Long errorPositionInStream = streamErrorOccurrence.positionInStream();
            final String componentName = streamErrorOccurrence.componentName();
            final String source = streamErrorOccurrence.source();

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
                            " streamId: {} component: {} source: {} expected position: {} actual position: {}",
                        streamId, componentName, source, expectedPositionInStream, currentPositionInStream);

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }

    private boolean checkStreamStatusPositionNotChanged(final Long expectedPositionInStream, final Long currentPositionInStream) {
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
    public void markSameErrorHappened(final UUID existingStreamErrorId, final UUID streamId, final String source, final String component) {
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            streamStatusErrorPersistence.updateStreamErrorOccurredAt(existingStreamErrorId, streamId, source, component, connection);
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get connection to view-store", e);
        }
    }
}
