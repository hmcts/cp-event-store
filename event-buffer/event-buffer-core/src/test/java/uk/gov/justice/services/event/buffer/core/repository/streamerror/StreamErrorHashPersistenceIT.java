package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.sql.Connection;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorHashPersistenceIT {

    @Spy
    private StreamErrorHashRowMapper streamErrorHashRowMapper;
    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;
    private final TestJdbcDataSourceProvider testJdbcDataSourceProvider = new TestJdbcDataSourceProvider();
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();


    @BeforeEach
    public void cleanTables() {
        databaseCleaner.cleanViewStoreTables("framework", "stream_error_hash", "stream_error");
    }

    @Test
    public void shouldUpsertAndFindByHash() throws Exception {

        final StreamErrorHash streamErrorHash = aStreamErrorHash();

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try(final Connection connection = viewStoreDataSource.getConnection()) {
            final int rowsAffected = streamErrorHashPersistence.upsert(streamErrorHash, connection);
            assertThat(rowsAffected, is(1));
            assertThat(connection.isClosed(), is(false));
        }
        try(final Connection connection = viewStoreDataSource.getConnection()) {
            final Optional<StreamErrorHash> streamErrorHashOptional = streamErrorHashPersistence.findByHash(streamErrorHash.hash(), connection);
            assertThat(streamErrorHashOptional.isPresent(), is(true));
            assertThat(streamErrorHashOptional.get(), is(streamErrorHash));
        }
    }

    @Test
    public void shouldReturnEmptyIfNoHashFound() throws Exception {

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try(final Connection connection = viewStoreDataSource.getConnection()) {

            assertThat(streamErrorHashPersistence.findAll(connection).isEmpty(), is(true));

            final Optional<StreamErrorHash> streamErrorHashOptional = streamErrorHashPersistence.findByHash("some-non-existent-hash", connection);
            assertThat(streamErrorHashOptional, is(empty()));
        }
    }


    @Test
    public void shouldOnlyInsertOnceOnUpsert() throws Exception {

        final StreamErrorHash streamErrorHash = aStreamErrorHash();

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try(final Connection connection = viewStoreDataSource.getConnection()) {
            assertThat(streamErrorHashPersistence.upsert(streamErrorHash, connection), is(1));
            assertThat(streamErrorHashPersistence.upsert(streamErrorHash, connection), is(0));
            assertThat(streamErrorHashPersistence.upsert(streamErrorHash, connection), is(0));
            assertThat(streamErrorHashPersistence.upsert(streamErrorHash, connection), is(0));

        }
    }

    @Test
    public void shouldDeleteByHash() throws Exception {

        final StreamErrorHash streamErrorHash = aStreamErrorHash();

        final DataSource viewStoreDataSource = testJdbcDataSourceProvider.getViewStoreDataSource("framework");

        try(final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.upsert(streamErrorHash, connection);
            final Optional<StreamErrorHash> optionalStreamErrorHash = streamErrorHashPersistence.findByHash(streamErrorHash.hash(), connection);
            assertThat(optionalStreamErrorHash.isPresent(), is(true));
        }

        try(final Connection connection = viewStoreDataSource.getConnection()) {
            streamErrorHashPersistence.deleteHash(streamErrorHash.hash(), connection);
            final Optional<StreamErrorHash> optionalStreamErrorHash = streamErrorHashPersistence.findByHash(streamErrorHash.hash(), connection);
            assertThat(optionalStreamErrorHash.isPresent(), is(false));
        }
    }

    private StreamErrorHash aStreamErrorHash() {
        final String hash = "hash";
        final String exceptionClassName = "exceptionClassName";
        final String causeClassName = "causeClassName";
        final String javaClassName = "javaClassName";
        final String javaMethod = "javaMethod";
        final int javaLineNumber = 982734;

        return new StreamErrorHash(
                hash,
                exceptionClassName,
                Optional.of(causeClassName),
                javaClassName,
                javaMethod,
                javaLineNumber);
    }
}