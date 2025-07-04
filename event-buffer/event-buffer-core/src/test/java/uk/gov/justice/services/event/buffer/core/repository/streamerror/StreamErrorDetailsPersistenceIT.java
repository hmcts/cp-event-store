package uk.gov.justice.services.event.buffer.core.repository.streamerror;

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
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
public class StreamErrorDetailsPersistenceIT {

    private final TestJdbcDataSourceProvider testJdbcDataSourceProvider = new TestJdbcDataSourceProvider();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    @Spy
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;
    @InjectMocks
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;
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
        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
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
            streamErrorDetailsPersistence.insert(streamErrorDetails, connection);
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final Optional<StreamErrorDetails> streamErrorDetailsOptional = streamErrorDetailsPersistence.findById(
                    streamErrorDetails.id(),
                    connection);

            if (streamErrorDetailsOptional.isPresent()) {
                assertThat(streamErrorDetailsOptional.get(), is(streamErrorDetails));
            } else {
                fail();
            }
        }
    }

    @Test
    public void shouldInsertAndFindByStreamId() throws Exception {
        final UUID streamId = randomUUID();
        final String hash = "sdlksdljfsdlf87236662846";

        final StreamErrorDetails streamErrorDetails_1 = new StreamErrorDetails(
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
        final StreamErrorDetails streamErrorDetails_2 = new StreamErrorDetails(
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
        final StreamErrorDetails streamErrorDetails_3 = new StreamErrorDetails(
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
            streamErrorDetailsPersistence.insert(streamErrorDetails_1, connection);
            streamErrorDetailsPersistence.insert(streamErrorDetails_2, connection);
            streamErrorDetailsPersistence.insert(streamErrorDetails_3, connection);
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorDetails> streamErrorDetailsList = streamErrorDetailsPersistence.findByStreamId(
                    streamId,
                    connection);

            assertThat(streamErrorDetailsList.size(), is(2));
            assertThat(streamErrorDetailsList.get(0), is(streamErrorDetails_1));
            assertThat(streamErrorDetailsList.get(1), is(streamErrorDetails_3));
        }
    }

    @Test
    public void shouldDeleteStreamErrorPlusAnyOrphanedHashes() throws Exception {

        final String hash = "this-is-the-hash";
        final UUID streamId_1 = fromString("ac2ba380-548a-445d-b6a9-56e785bd26ff");
        final UUID streamId_2 = fromString("e2de717c-9bf4-432b-ab0e-4e4e0484f35e");

        final StreamErrorDetails streamErrorDetails_1 = new StreamErrorDetails(
                randomUUID(),
                hash,
                "some-exception-message-1",
                of("cause-message-1"),
                "event-name-1",
                randomUUID(),
                streamId_1,
                111L,
                new UtcClock().now(),
                "stack-trace-1",
                "component-name-1",
                "source-1"
        );

        final StreamErrorDetails streamErrorDetails_2 = new StreamErrorDetails(
                randomUUID(),
                hash,
                "some-exception-message-2",
                of("cause-message-2"),
                "event-name-2",
                randomUUID(),
                streamId_2,
                22222L,
                new UtcClock().now(),
                "stack-trace-2",
                "component-name-2",
                "source-2"
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

        // save some errors with their hash
        try (final Connection connection = viewStoreDataSource.getConnection()) {

            // should only save one row regardless of how many times we call it
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            streamErrorHashPersistence.upsert(streamErrorHash, connection);

            streamErrorDetailsPersistence.insert(streamErrorDetails_1, connection);
            streamErrorDetailsPersistence.insert(streamErrorDetails_2, connection);

        }

        // make sure they saved correctly
        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);
            assertThat(streamErrorDetails.size(), is(2));
            assertThat(streamErrorDetails, hasItems(streamErrorDetails_1, streamErrorDetails_2));

            final List<StreamErrorHash> streamErrorHashes = streamErrorHashPersistence.findAll(connection);
            assertThat(streamErrorHashes.size(), is(1));
            assertThat(streamErrorHashes, hasItem(streamErrorHash));
        }

        // delete one error
        try (final Connection connection = viewStoreDataSource.getConnection()) {

            streamErrorDetailsPersistence.deleteBy(
                    streamErrorDetails_1.streamId(),
                    streamErrorDetails_1.source(),
                    streamErrorDetails_1.componentName(),
                    connection);
            streamErrorHashPersistence.deleteOrphanedHashes(connection);
        }

        // make sure we still have the second error plus the hash
        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);
            assertThat(streamErrorDetails.size(), is(1));
            assertThat(streamErrorDetails, hasItem(streamErrorDetails_2));

            final List<StreamErrorHash> streamErrorHashes = streamErrorHashPersistence.findAll(connection);
            assertThat(streamErrorHashes.size(), is(1));
            assertThat(streamErrorHashes, hasItem(streamErrorHash));
        }

        // delete the second error
        try (final Connection connection = viewStoreDataSource.getConnection()) {

            streamErrorDetailsPersistence.deleteBy(
                    streamErrorDetails_2.streamId(),
                    streamErrorDetails_2.source(),
                    streamErrorDetails_2.componentName(),
                    connection);
            streamErrorHashPersistence.deleteOrphanedHashes(connection);
        }

        // now both the error and the has should have gone
        try (final Connection connection = viewStoreDataSource.getConnection()) {
            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);
            assertThat(streamErrorDetails.isEmpty(), is(true));

            final List<StreamErrorHash> streamErrorHashes = streamErrorHashPersistence.findAll(connection);
            assertThat(streamErrorHashes.isEmpty(), is(true));
        }
    }
}