package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorOccurrencePersistenceIT {

    private final TestJdbcDataSourceProvider testJdbcDataSourceProvider = new TestJdbcDataSourceProvider();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    @Spy
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;
    @InjectMocks
    private StreamErrorOccurrencePersistence streamErrorOccurrencePersistence;
    @Spy
    private StreamErrorHashRowMapper streamErrorHashRowMapper;
    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @BeforeEach
    public void cleanTables() {
        databaseCleaner.cleanViewStoreTables("framework", "stream_error_hash", "stream_error");
    }

    @Test
    public void shouldInsertAndFindById() throws Exception {
        final String hash = "sdlksdljfsdlf87236662846";
        final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message",
                of("cause-message"),
                "event-name",
                randomUUID(),
                randomUUID(),
                23423L,
                new UtcClock().now(),
                "stack-trace",
                "component-name",
                "source"
        );

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                hash,
                "some-exception-class-name",
                of("some-cause-class-name"),
                "java-class-name",
                "java-method",
                213
        );

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence, connection);
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final Optional<StreamErrorOccurrence> streamErrorDetailsOptional = streamErrorOccurrencePersistence.findById(
                    streamErrorOccurrence.id(),
                    connection);

            if (streamErrorDetailsOptional.isPresent()) {
                assertThat(streamErrorDetailsOptional.get(), is(streamErrorOccurrence));
            } else {
                fail();
            }
        }
    }

    @Test
    public void shouldInsertAndFindByStreamId() throws Exception {
        final UUID streamId = randomUUID();
        final String hash = "sdlksdljfsdlf87236662846";

        final StreamErrorOccurrence streamErrorOccurrence_1 = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message_1",
                of("cause-message_1"),
                "event-name_1",
                randomUUID(),
                streamId,
                987342987L,
                new UtcClock().now(),
                "stack-trace_1",
                "component-name_1",
                "source_1"
        );
        final StreamErrorOccurrence streamErrorOccurrence_2 = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message_2",
                of("cause-message_2"),
                "event-name_2",
                randomUUID(),
                randomUUID(),
                97878L,
                new UtcClock().now(),
                "stack-trace_2",
                "component-name_2",
                "source_2"
        );
        final StreamErrorOccurrence streamErrorOccurrence_3 = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message_3",
                of("cause-message_3"),
                "event-name_3",
                randomUUID(),
                streamId,
                8793877L,
                new UtcClock().now(),
                "stack-trace_3",
                "component-name_3",
                "source_3"
        );

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                hash,
                "some-exception-class-name",
                of("some-cause-class-name"),
                "java-class-name",
                "java-method",
                213
        );

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence_1, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence_2, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence_3, connection);
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorOccurrence> streamErrorOccurrenceList = streamErrorOccurrencePersistence.findByStreamId(
                    streamId,
                    connection);

            assertThat(streamErrorOccurrenceList.size(), is(2));
            assertThat(streamErrorOccurrenceList.get(0), is(streamErrorOccurrence_1));
            assertThat(streamErrorOccurrenceList.get(1), is(streamErrorOccurrence_3));
        }
    }

    @Test
    public void shouldDeleteStreamErrorByIdAndReturnItsHash() throws Exception {
        final String hash = "sdlksdljfsdlf87236662846";
        final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message",
                of("cause-message"),
                "event-name",
                randomUUID(),
                randomUUID(),
                23423L,
                new UtcClock().now(),
                "stack-trace",
                "component-name",
                "source"
        );

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                hash,
                "some-exception-class-name",
                of("some-cause-class-name"),
                "java-class-name",
                "java-method",
                213
        );

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence, connection);
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorOccurrence> allStreamErrorDetails = streamErrorOccurrencePersistence.findAll(connection);

            assertThat(allStreamErrorDetails.size(), is(1));
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final String returnedHash = streamErrorOccurrencePersistence.deleteErrorAndGetHash(
                    streamErrorOccurrence.id(),
                    connection);

            assertThat(returnedHash, is(hash));
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorOccurrence> allStreamErrorDetails = streamErrorOccurrencePersistence.findAll(connection);
            assertThat(allStreamErrorDetails.size(), is(0));
        }
    }

    @Test
    public void shouldCalculateIfAnyErrorsExistForGivenHash() throws Exception {
        final String hash = "sdlksdljfsdlf872237jkjshd2846";

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");
        try(final Connection connection = viewStoreDataSource.getConnection()) {
            assertThat(streamErrorOccurrencePersistence.noErrorsExistFor(hash, connection), is(true));
        }

        final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                randomUUID(),
                hash,
                "some-exception-message",
                of("cause-message"),
                "event-name",
                randomUUID(),
                randomUUID(),
                23423L,
                new UtcClock().now(),
                "stack-trace",
                "component-name",
                "source"
        );

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                hash,
                "some-exception-class-name",
                of("some-cause-class-name"),
                "java-class-name",
                "java-method",
                213
        );

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorOccurrencePersistence.insert(streamErrorOccurrence, connection);
        }

        try(final Connection connection = viewStoreDataSource.getConnection()) {
            assertThat(streamErrorOccurrencePersistence.noErrorsExistFor(hash, connection), is(false));
        }
    }
}