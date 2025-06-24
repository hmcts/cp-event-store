package uk.gov.justice.services.resources.repository;

import java.time.ZonedDateTime;
import java.util.List;
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
public class StreamStatusReadRepositoryIT {

    private static final String FRAMEWORK = "framework";
    private static final String LISTING_SOURCE = "listing";
    private static final String EVENT_LISTENER_COMPONENT = "EVENT_LISTENER";
    private static final String EVENT_INDEXER_COMPONENT = "EVENT_INDEXER";
    private static final UUID STREAM_1_ID = randomUUID();
    private static final UUID STREAM_2_ID = randomUUID();
    private static final UUID STREAM_3_ID = randomUUID();
    private static final ZonedDateTime UPDATED_AT_1 = new UtcClock().now().minusDays(2);
    private static final ZonedDateTime UPDATED_AT_2 = UPDATED_AT_1.plusMinutes(1);
    private static final ZonedDateTime UPDATED_AT_3 = UPDATED_AT_1.plusMinutes(2);
    private static final String ERROR_1_HASH = "hash-1";
    private static final String ERROR_2_HASH = "hash-2";

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
    public void cleanDatabase() throws Exception {
        new DatabaseCleaner().cleanStreamStatusTable(FRAMEWORK);
        setupStreams();
    }

    private void setupStreams() throws Exception {
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamStatusReadRepository.findByErrorHash(ERROR_1_HASH).isEmpty(), is(true));
        assertThat(streamStatusReadRepository.findByStreamId(STREAM_1_ID).isEmpty(), is(true));
        assertThat(streamStatusReadRepository.findErrorStreams().isEmpty(), is(true));

        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT_1, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, UPDATED_AT_2, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_2_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT_1, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_3_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT_3, false);
        insertEntriesToStreamErrorHash(ERROR_1_HASH, ERROR_2_HASH, viewStoreDataSource);
        /*
            stream-1:
                event-1: listing/event_listener, error-hash-1
                event-1: listing/event_indexer, error-hash-1
         */
        insertEntryToStreamError(STREAM_1_ID, ERROR_1_HASH, 1L, viewStoreDataSource, LISTING_SOURCE, EVENT_LISTENER_COMPONENT);
        insertEntryToStreamError(STREAM_1_ID, ERROR_1_HASH, 1L, viewStoreDataSource, LISTING_SOURCE, EVENT_INDEXER_COMPONENT);
        insertEntryToStreamError(STREAM_2_ID, ERROR_2_HASH, 1L, viewStoreDataSource, LISTING_SOURCE, EVENT_LISTENER_COMPONENT);
        insertEntryToStreamError(STREAM_3_ID, ERROR_1_HASH, 1L, viewStoreDataSource, LISTING_SOURCE, EVENT_LISTENER_COMPONENT);
    }

    @Test
    public void shouldQueryAllStreamsInDescOrderOfUpdatedAtByErrorHash() {
        final List<StreamStatus> streamStatuses = streamStatusReadRepository.findByErrorHash(ERROR_1_HASH);

        assertThat(streamStatuses.size(), is(3));
        final StreamStatus streamStatus1 = streamStatuses.get(0);
        assertThat(streamStatus1.streamId(), is(STREAM_3_ID));
        assertThat(streamStatus1.latestKnownPosition(), is(0L));
        assertThat(streamStatus1.position(), is(0L));
        assertThat(streamStatus1.updatedAt(), is(UPDATED_AT_3));
        assertThat(streamStatus1.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus1.source(), is(LISTING_SOURCE));
        assertThat(streamStatus1.isUpToDate(), is(false));
        assertTrue(streamStatus1.streamErrorId().isEmpty());
        assertTrue(streamStatus1.streamErrorPosition().isEmpty());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.updatedAt(), is(UPDATED_AT_2));
        assertThat(streamStatus2.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertTrue(streamStatus2.streamErrorId().isEmpty());
        assertTrue(streamStatus2.streamErrorPosition().isEmpty());

        final StreamStatus streamStatus3 = streamStatuses.get(2);
        assertThat(streamStatus3.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus3.latestKnownPosition(), is(0L));
        assertThat(streamStatus3.position(), is(0L));
        assertThat(streamStatus3.updatedAt(), is(UPDATED_AT_1));
        assertThat(streamStatus3.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus3.source(), is(LISTING_SOURCE));
        assertThat(streamStatus3.isUpToDate(), is(false));
        assertTrue(streamStatus3.streamErrorId().isEmpty());
        assertTrue(streamStatus3.streamErrorPosition().isEmpty());
    }

    @Test
    public void shouldQueryAllStreamsInDescOrderOfUpdatedAtByStreamId() {
        final List<StreamStatus> streamStatuses = streamStatusReadRepository.findByStreamId(STREAM_1_ID);

        assertThat(streamStatuses.size(), is(2));
        final StreamStatus streamStatus1 = streamStatuses.get(0);
        assertThat(streamStatus1.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus1.latestKnownPosition(), is(0L));
        assertThat(streamStatus1.position(), is(0L));
        assertThat(streamStatus1.updatedAt(), is(UPDATED_AT_2));
        assertThat(streamStatus1.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus1.source(), is(LISTING_SOURCE));
        assertThat(streamStatus1.isUpToDate(), is(false));
        assertTrue(streamStatus1.streamErrorId().isEmpty());
        assertTrue(streamStatus1.streamErrorPosition().isEmpty());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.updatedAt(), is(UPDATED_AT_1));
        assertThat(streamStatus2.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertTrue(streamStatus2.streamErrorId().isEmpty());
        assertTrue(streamStatus2.streamErrorPosition().isEmpty());
    }

    private void insertEntriesToStreamErrorHash(String error1Hash, String error2Hash, DataSource dataSource) throws Exception {
        final StreamErrorHash streamError1Hash = new StreamErrorHash(error1Hash, "java.lang.NullPointerException", Optional.empty(), "java.lang.NullPointerException", "find", 1);
        final StreamErrorHash streamError2Hash = new StreamErrorHash(error2Hash, "java.lang.IllegalArgumentException", Optional.empty(), "java.lang.IllegalArgumentException", "find1", 2);
        streamErrorHashPersistence.upsert(streamError1Hash, dataSource.getConnection());
        streamErrorHashPersistence.upsert(streamError2Hash, dataSource.getConnection());
    }

    private void insertEntryToStreamError(final UUID stream1Id, final String error1Hash,
                                          final Long position, final DataSource dataSource,
                                          final String source, final String component) throws Exception {
        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                randomUUID(), error1Hash, "some-exception-message", empty(),
                "event-name", randomUUID(), stream1Id, position,
                new UtcClock().now(), "stack-trace",
                component, source
        );
        streamErrorDetailsPersistence.insert(streamErrorDetails, dataSource.getConnection());
    }
}
