package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrencePersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamErrorRepositoryIT {

    private final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");

    @Spy
    private StreamErrorHashRowMapper streamErrorHashRowMapper;

    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @Spy
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;

    @InjectMocks
    private StreamErrorOccurrencePersistence streamErrorOccurrencePersistence;

    @Spy
    private StreamErrorPersistence streamErrorPersistence;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider = new ViewStoreJdbcDataSourceProvider();

    @Spy
    private StreamStatusErrorPersistence streamStatusErrorPersistence = new StreamStatusErrorPersistence();

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamErrorRepository streamErrorRepository;

    @BeforeEach
    public void setup() {
        new DatabaseCleaner().cleanViewStoreTables(
                "framework",
                "stream_status",
                "stream_buffer",
                "stream_error");
        setField(streamErrorPersistence, "streamErrorHashPersistence", streamErrorHashPersistence);
        setField(streamErrorPersistence, "streamErrorOccurrencePersistence", streamErrorOccurrencePersistence);
        setField(streamStatusErrorPersistence, "clock", new UtcClock());
    }

    @Test
    public void shouldSaveNewStreamErrorAndUpdateStreamStatusTable() throws Exception {

        final long streamErrorPosition = 234L;
        final long currentStreamPosition = 233L;
        final StreamError streamError = aStreamError(streamErrorPosition);
        final UUID streamId = streamError.streamErrorOccurrence().streamId();
        final String source = streamError.streamErrorOccurrence().source();
        final String componentName = streamError.streamErrorOccurrence().componentName();


        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        insertStreamStatus(streamId, currentStreamPosition, source, componentName, viewStoreDataSource.getConnection());

        streamErrorRepository.markStreamAsErrored(streamError, currentStreamPosition);

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final Optional<StreamError> streamErrorOptional = streamErrorPersistence.findByErrorId(streamError.streamErrorOccurrence().id(), connection);
            assertThat(streamErrorOptional, is(of(streamError)));
        }

        final Optional<StreamStatusErrorDetails> streamStatusErrorDetails = findErrorInStreamStatusTable(streamError.streamErrorOccurrence().id());

        if (streamStatusErrorDetails.isPresent()) {
            assertThat(streamStatusErrorDetails.get().streamErrorId, is(streamError.streamErrorOccurrence().id()));
            assertThat(streamStatusErrorDetails.get().streamId, is(streamError.streamErrorOccurrence().streamId()));
            assertThat(streamStatusErrorDetails.get().streamErrorPosition, is(streamErrorPosition));
            assertThat(streamStatusErrorDetails.get().source, is(streamError.streamErrorOccurrence().source()));
            assertThat(streamStatusErrorDetails.get().component, is(streamError.streamErrorOccurrence().componentName()));
        } else {
            fail();
        }
    }

    @Test
    public void shouldReturnStreamErrorIdWhenLockingRowInStreamStatusIfErrorExistsOnThatStream() throws Exception {

        final long streamErrorPosition = 234L;
        final long currentStreamPosition = 233L;
        final StreamError streamError = aStreamError(streamErrorPosition);
        final UUID streamId = streamError.streamErrorOccurrence().streamId();
        final String source = streamError.streamErrorOccurrence().source();
        final String componentName = streamError.streamErrorOccurrence().componentName();

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        insertStreamStatus(streamId, currentStreamPosition, source, componentName, viewStoreDataSource.getConnection());

        streamErrorRepository.markStreamAsErrored(streamError, currentStreamPosition);

        final NewStreamStatusRepository newStreamStatusRepository = new NewStreamStatusRepository();

        setField(newStreamStatusRepository, "streamStatusRowMapper", new NewStreamStatusRowMapper());
        setField(newStreamStatusRepository, "viewStoreJdbcDataSourceProvider", viewStoreJdbcDataSourceProvider);


        final StreamUpdateContext streamUpdateContext = newStreamStatusRepository.lockStreamAndGetStreamUpdateContext(streamId, source, componentName, streamErrorPosition);

        assertThat(streamUpdateContext.streamErrorId(), is(of(streamError.streamErrorOccurrence().id())));

    }

    @Test
    public void shouldSaveNewStreamErrorAndUpdateStreamStatusTableWhenStreamExistsInStreamStatusTable() throws Exception {

        final long streamErrorPosition = 234L;
        final long currentStreamPosition = 233L;
        final StreamError streamError = aStreamError(streamErrorPosition);
        final UUID streamId = streamError.streamErrorOccurrence().streamId();
        final String source = streamError.streamErrorOccurrence().source();
        final String componentName = streamError.streamErrorOccurrence().componentName();

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        insertStreamStatus(streamId, currentStreamPosition, source, componentName, viewStoreDataSource.getConnection());

        streamErrorRepository.markStreamAsErrored(streamError, currentStreamPosition);

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final Optional<StreamError> streamErrorOptional = streamErrorPersistence.findByErrorId(streamError.streamErrorOccurrence().id(), connection);
            assertThat(streamErrorOptional, is(of(streamError)));
        }

        final Optional<StreamStatusErrorDetails> streamStatusErrorDetails = findErrorInStreamStatusTable(streamError.streamErrorOccurrence().id());

        if (streamStatusErrorDetails.isPresent()) {
            assertThat(streamStatusErrorDetails.get().streamErrorId, is(streamError.streamErrorOccurrence().id()));
            assertThat(streamStatusErrorDetails.get().streamId, is(streamError.streamErrorOccurrence().streamId()));
            assertThat(streamStatusErrorDetails.get().streamErrorPosition, is(streamErrorPosition));
            assertThat(streamStatusErrorDetails.get().source, is(streamError.streamErrorOccurrence().source()));
            assertThat(streamStatusErrorDetails.get().component, is(streamError.streamErrorOccurrence().componentName()));
        } else {
            fail();
        }
    }

    @Test
    public void shouldUpdateStreamErrorOccurredAtWhenMarkingSameErrorHappened() throws Exception {

        final long streamErrorPosition = 234L;
        final long currentStreamPosition = 233L;
        final StreamError streamError = aStreamError(streamErrorPosition);
        final UUID streamId = streamError.streamErrorOccurrence().streamId();
        final UUID streamErrorId = streamError.streamErrorOccurrence().id();
        final String source = streamError.streamErrorOccurrence().source();
        final String componentName = streamError.streamErrorOccurrence().componentName();

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        insertStreamStatus(streamId, currentStreamPosition, source, componentName, viewStoreDataSource.getConnection());
        streamErrorRepository.markStreamAsErrored(streamError, currentStreamPosition);

        final Optional<Timestamp> occurredAtBefore = getStreamErrorOccurredAt(streamErrorId);
        assertThat(occurredAtBefore.isPresent(), is(true));

        // run
        streamErrorRepository.markSameErrorHappened(streamErrorId, streamId, source, componentName);

        // Verify
        final Optional<Timestamp> occurredAtAfter = getStreamErrorOccurredAt(streamErrorId);
        assertThat(occurredAtAfter.isPresent(), is(true));
        assertThat(occurredAtAfter.get().after(occurredAtBefore.get()) || occurredAtAfter.get().equals(occurredAtBefore.get()), is(true));
    }

    private Optional<StreamStatusErrorDetails> findErrorInStreamStatusTable(final UUID streamErrorId) throws SQLException {

        final String sql = """
                SELECT
                    stream_id,
                    stream_error_position,
                    source,
                    component
                FROM stream_status
                WHERE stream_error_id = ?
                """;

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, streamErrorId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    final UUID streamId = (UUID) resultSet.getObject("stream_id");
                    final long streamErrorPosition = resultSet.getLong("stream_error_position");
                    final String source = resultSet.getString("source");
                    final String component = resultSet.getString("component");

                    final StreamStatusErrorDetails streamStatusErrorDetails = new StreamStatusErrorDetails(
                            streamErrorId,
                            streamId,
                            streamErrorPosition,
                            source,
                            component
                    );

                    return of(streamStatusErrorDetails);
                }
            }

            return empty();
        }
    }

    private StreamError aStreamError(long streamErrorPosition) {
        final String hash = "some-hash";
        return new StreamError(aStreamErrorDetails(hash, streamErrorPosition), aStreamErrorHash(hash));
    }

    private StreamErrorOccurrence aStreamErrorDetails(final String hash, final long streamErrorPosition) {

        final UUID streamId = randomUUID();
        final String componentName = "some-component";
        final String source = "some-source";

        final UtcClock utcClock = new UtcClock();
        return new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message",
                empty(),
                "event-name",
                randomUUID(),
                streamId,
                streamErrorPosition,
                utcClock.now(),
                "stack-trace",
                componentName,
                source,
                utcClock.now()
        );
    }

    private StreamErrorHash aStreamErrorHash(final String hash) {
        return new StreamErrorHash(
                hash,
                "exception-class-name",
                empty(),
                "java-class-name",
                "java-method",
                23
        );
    }

    record StreamStatusErrorDetails(
            UUID streamErrorId,
            UUID streamId,
            long streamErrorPosition,
            String source,
            String component) {
    }

    public void insertStreamStatus(
            final UUID streamId,
            final Long positionInStream,
            final String source,
            final String componentName,
            final Connection connection) throws SQLException {


        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                 INSERT INTO stream_status (
                                stream_id,
                                position,
                                source,
                                component, 
                                discovered_at
                            ) VALUES (?, ?, ?, ?, ?)
                """)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setLong(2, positionInStream);
            preparedStatement.setString(3, source);
            preparedStatement.setString(4, componentName);
            preparedStatement.setTimestamp(5, toSqlTimestamp(new UtcClock().now()));

            preparedStatement.executeUpdate();
        }
    }

    private Optional<Timestamp> getStreamErrorOccurredAt(final UUID streamErrorId) throws SQLException {

        final String sql = """
                SELECT occurred_at
                FROM stream_error
                WHERE id = ?
                """;

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, streamErrorId);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return of(resultSet.getTimestamp("occurred_at"));
                }
            }
        }

        return empty();
    }
}