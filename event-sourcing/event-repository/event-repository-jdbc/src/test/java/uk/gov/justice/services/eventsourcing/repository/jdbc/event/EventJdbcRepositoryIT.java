package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.test.utils.events.EventBuilder.eventBuilder;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventInsertionStrategy;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;
import uk.gov.justice.services.test.utils.persistence.SettableEventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventJdbcRepositoryIT {

    private static final UUID STREAM_ID = randomUUID();
    private static final Long SEQUENCE_ID = 5L;

    private static final String FRAMEWORK_CONTEXT_NAME = "framework";

    @SuppressWarnings("unused")
    @Spy
    private EventInsertionStrategy eventInsertionStrategy = new EventInsertionStrategy();

    @SuppressWarnings("unused")
    @Spy
    private JdbcResultSetStreamer jdbcResultSetStreamer = new JdbcResultSetStreamer();

    @SuppressWarnings("unused")
    @Spy
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory = new PreparedStatementWrapperFactory();

    @Spy
    private SettableEventStoreDataSourceProvider eventStoreDDataSourceProvider = new SettableEventStoreDataSourceProvider();

    @SuppressWarnings("unused")
    @Mock
    private Logger logger;

    @InjectMocks
    private EventJdbcRepository jdbcRepository;

    private DataSource dataSource;

    @BeforeEach
    public void initialize() throws Exception {

        dataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        eventStoreDDataSourceProvider.setDataSource(dataSource);

        new DatabaseCleaner().cleanEventStoreTables(FRAMEWORK_CONTEXT_NAME);
    }

    @AfterEach
    public void after() throws SQLException {
        dataSource.getConnection().close();
    }

    @Test
    public void shouldStoreEventsUsingInsert() throws InvalidPositionException {

        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(SEQUENCE_ID).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(SEQUENCE_ID + 1).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(SEQUENCE_ID + 2).build());

        final Stream<Event> events = jdbcRepository.findByStreamIdOrderByPositionAsc(STREAM_ID);
        final Stream<Event> events2 = jdbcRepository.findByStreamIdFromPositionOrderByPositionAsc(STREAM_ID, SEQUENCE_ID + 1);
        final Long latestSequenceId = jdbcRepository.getStreamSize(STREAM_ID);

        assertThat(events.count(), equalTo(3L));
        assertThat(events2.count(), equalTo(2L));
        assertThat(latestSequenceId, equalTo(7L));
    }

    @Test
    public void shouldFindById() throws Exception {

        final UUID id = randomUUID();

        final Event event = eventBuilder().withId(id).withStreamId(STREAM_ID).withPositionInStream(SEQUENCE_ID).build();

        jdbcRepository.insert(event);

        final Optional<Event> foundEvent = jdbcRepository.findById(id);

        if (foundEvent.isPresent()) {
            assertThat(foundEvent.get(), is(event));
        } else {
            fail();
        }
    }

    @Test
    public void shouldReturnEventsByStreamIdOrderedBySequenceId() throws InvalidPositionException {

        jdbcRepository.insert(eventBuilder().withStreamId(randomUUID()).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(7L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(4L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(2L).build());

        try (final Stream<Event> events = jdbcRepository.findByStreamIdOrderByPositionAsc(STREAM_ID)) {
            final List<Event> eventList = events.collect(toList());
            assertThat(eventList, hasSize(3));
            assertThat(eventList.get(0).getPositionInStream(), is(2L));
            assertThat(eventList.get(1).getPositionInStream(), is(4L));
            assertThat(eventList.get(2).getPositionInStream(), is(7L));
        }
    }

    @Test
    public void shouldStoreAndReturnDateCreated() throws InvalidPositionException {

        final Event event = eventBuilder().withPositionInStream(1L).build();
        jdbcRepository.insert(event);

        Stream<Event> events = jdbcRepository.findByStreamIdOrderByPositionAsc(event.getStreamId());

        final List<Event> eventList = events.collect(toList());
        assertThat(eventList, hasSize(1));
        assertThat(eventList.get(0).getCreatedAt(), is(event.getCreatedAt()));
    }

    @Test
    public void shouldReturnEventsByStreamIdFromSequenceIdOrderBySequenceId() throws InvalidPositionException {

        jdbcRepository.insert(eventBuilder().withStreamId(randomUUID()).withPositionInStream(5L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(7L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(4L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(3L).build());

        final Stream<Event> events = jdbcRepository.findByStreamIdFromPositionOrderByPositionAsc(STREAM_ID, 4L);
        final List<Event> eventList = events.collect(toList());
        assertThat(eventList, hasSize(2));
        assertThat(eventList.get(0).getPositionInStream(), is(4L));
        assertThat(eventList.get(1).getPositionInStream(), is(7L));
    }

    @Test
    public void shouldReturnEventsByStreamIdFromPositionOrderByPositionByPage() throws InvalidPositionException {

        final int pageSize = 2;

        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(7L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(4L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(3L).build());

        final Stream<Event> events = jdbcRepository.findByStreamIdFromPositionOrderByPositionAsc(STREAM_ID, 3L, pageSize);
        final List<Event> eventList = events.collect(toList());
        assertThat(eventList, hasSize(2));
        assertThat(eventList.get(0).getPositionInStream(), is(3L));
        assertThat(eventList.get(1).getPositionInStream(), is(4L));
    }

    @Test
    public void shouldReturnAllEventsOrderedBySequenceId() throws InvalidPositionException {

        jdbcRepository.insert(eventBuilder().withStreamId(randomUUID()).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(4L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(2L).build());

        final Stream<Event> events = jdbcRepository.findAll();

        final List<Event> eventList = events.collect(toList());
        assertThat(eventList, hasSize(3));
        assertThat(eventList.get(0).getPositionInStream(), is(1L));
        assertThat(eventList.get(1).getPositionInStream(), is(2L));
        assertThat(eventList.get(2).getPositionInStream(), is(4L));
    }

    @Test
    public void shouldReturnStreamOfStreamIds() throws Exception {

        final UUID streamId1 = randomUUID();
        final UUID streamId2 = randomUUID();
        final UUID streamId3 = randomUUID();

        jdbcRepository.insert(eventBuilder().withStreamId(streamId1).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(streamId2).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(streamId3).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(streamId1).withPositionInStream(2L)
                .build());

        final Stream<UUID> streamIds = jdbcRepository.getStreamIds();

        final List<UUID> streamIdList = streamIds.collect(toList());

        assertThat(streamIdList, hasSize(3));
        assertThat(streamIdList, hasItem(streamId1));
        assertThat(streamIdList, hasItem(streamId2));
        assertThat(streamIdList, hasItem(streamId3));
    }

    @Test
    public void shouldClearStream() throws InvalidPositionException {

        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(1L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(2L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(3L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(4L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(5L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(6L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(7L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(8L).build());
        jdbcRepository.insert(eventBuilder().withStreamId(STREAM_ID).withPositionInStream(9L).build());

        final Long latestSequenceId = jdbcRepository.getStreamSize(STREAM_ID);
        assertThat(latestSequenceId, equalTo(9L));

        jdbcRepository.clear(STREAM_ID);

        final Stream<Event> emptyEvents = jdbcRepository.findByStreamIdOrderByPositionAsc(STREAM_ID);
        assertThat(emptyEvents.count(), equalTo(0L));

        final Long deletedStreamLatestSequenceId = jdbcRepository.getStreamSize(STREAM_ID);
        assertThat(deletedStreamLatestSequenceId, equalTo(0L));
    }
}
