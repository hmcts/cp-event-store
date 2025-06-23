package uk.gov.justice.services.resources.repository;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamResponseStatusReadRepositoryIT {

    private static final String FRAMEWORK = "framework";

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @InjectMocks
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;

    @Spy
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @InjectMocks
    private StreamStatusReadRepository streamStatusReadRepository;

    @BeforeEach
    public void cleanDatabase() {
        new DatabaseCleaner().cleanStreamStatusTable(FRAMEWORK);
    }

    @Nested
    class FindByErrorHashTest {

        private static final String SOURCE = "some-source";
        private static final String COMPONENT_NAME = "some-component-name";

        @Test
        public void shouldQueryAllStreamsInDescOrderOfUpdatedAtByErrorHash() throws Exception {
            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID stream1Id = randomUUID();
            final UUID stream2Id = randomUUID();
            final UUID stream3Id = randomUUID();
            final boolean upToDate = false;
            final ZonedDateTime updatedAt = new UtcClock().now().minusDays(2);
            final ZonedDateTime updatedAtForStream3 = updatedAt.plusMinutes(1);
            final String error1Hash = "hash-1";
            final String error2Hash = "hash-2";

            assertThat(streamStatusReadRepository.findBy(error1Hash).isEmpty(), is(true));

            newStreamStatusRepository.insertIfNotExists(stream1Id, SOURCE, COMPONENT_NAME, updatedAt, upToDate);
            newStreamStatusRepository.insertIfNotExists(stream2Id, SOURCE, COMPONENT_NAME, updatedAt, upToDate);
            newStreamStatusRepository.insertIfNotExists(stream3Id, SOURCE, COMPONENT_NAME, updatedAtForStream3, upToDate);
            insertEntriesToStreamErrorHash(error1Hash, error2Hash, viewStoreDataSource);
            insertEntryToStreamError(stream1Id, error1Hash, 1L, viewStoreDataSource);
            insertEntryToStreamError(stream1Id, error1Hash, 2L, viewStoreDataSource);
            insertEntryToStreamError(stream2Id, error2Hash, 1L, viewStoreDataSource);
            insertEntryToStreamError(stream3Id, error1Hash, 1L, viewStoreDataSource);

            final List<StreamStatus> streamStatuses = streamStatusReadRepository.findBy(error1Hash);

            assertThat(streamStatuses.size(), is(2));
            final StreamStatus streamStatus1 = streamStatuses.get(0);
            assertThat(streamStatus1.streamId(), is(stream3Id));
            assertThat(streamStatus1.latestKnownPosition(), is(0L));
            assertThat(streamStatus1.position(), is(0L));
            assertThat(streamStatus1.updatedAt(), is(updatedAtForStream3));
            assertThat(streamStatus1.component(), is(COMPONENT_NAME));
            assertThat(streamStatus1.source(), is(SOURCE));
            assertThat(streamStatus1.isUpToDate(), is(upToDate));
            assertTrue(streamStatus1.streamErrorId().isEmpty());
            assertTrue(streamStatus1.streamErrorPosition().isEmpty());

            final StreamStatus streamStatus2 = streamStatuses.get(1);
            assertThat(streamStatus2.streamId(), is(stream1Id));
            assertThat(streamStatus2.latestKnownPosition(), is(0L));
            assertThat(streamStatus2.position(), is(0L));
            assertThat(streamStatus2.updatedAt(), is(updatedAt));
            assertThat(streamStatus2.component(), is(COMPONENT_NAME));
            assertThat(streamStatus2.source(), is(SOURCE));
            assertThat(streamStatus2.isUpToDate(), is(upToDate));
            assertTrue(streamStatus2.streamErrorId().isEmpty());
            assertTrue(streamStatus2.streamErrorPosition().isEmpty());
        }

        private void insertEntriesToStreamErrorHash(String error1Hash, String error2Hash, DataSource dataSource) throws Exception {
            final StreamErrorHash streamError1Hash = new StreamErrorHash(error1Hash.toString(), "java.lang.NullPointerException", Optional.empty(), "java.lang.NullPointerException", "find", 1);
            final StreamErrorHash streamError2Hash = new StreamErrorHash(error2Hash.toString(), "java.lang.IllegalArgumentException", Optional.empty(), "java.lang.IllegalArgumentException", "find1", 2);
            streamErrorHashPersistence.upsert(streamError1Hash, dataSource.getConnection());
            streamErrorHashPersistence.upsert(streamError2Hash, dataSource.getConnection());
        }

        private void insertEntryToStreamError(final UUID stream1Id, final String error1Hash,
                                              final Long position, final DataSource dataSource) throws Exception {
            final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                    randomUUID(), error1Hash.toString(), "some-exception-message", empty(),
                    "event-name", randomUUID(), stream1Id, position,
                    new UtcClock().now(), "stack-trace",
                    COMPONENT_NAME, SOURCE
            );
            streamErrorDetailsPersistence.insert(streamErrorDetails, dataSource.getConnection());
        }
    }
}
