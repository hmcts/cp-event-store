package uk.gov.justice.services.test.utils.events;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.events.EventBuilder.eventBuilder;
import static uk.gov.justice.services.test.utils.events.LinkedEventBuilder.linkedEventBuilder;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;
import uk.gov.justice.services.test.utils.persistence.TableCleaner;

import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

public class EventStoreDataAccessIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    private final EventStoreDataAccess eventStoreDataAccess = new EventStoreDataAccess(eventStoreDataSource);
    
    @Test
    public void shouldCreateAnEntryInTheEventLogTableUsingAnEvent() throws Exception {

        new TableCleaner().clean("event_log", eventStoreDataSource);

        assertThat(eventStoreDataAccess.findAllEvents(), is(emptyList()));

        final Event event_1 = eventBuilder().withName("event 1").build();
        final Event event_2 = eventBuilder().withName("event 2").build();
        final Event event_3 = eventBuilder().withName("event 3").build();

        eventStoreDataAccess.insertIntoEventLog(event_1);
        eventStoreDataAccess.insertIntoEventLog(event_2);
        eventStoreDataAccess.insertIntoEventLog(event_3);

        final List<Event> events = eventStoreDataAccess.findAllEvents();

        assertThat(events.size(), is(3));

        assertThat(events.get(0), is(event_1));
        assertThat(events.get(1), is(event_2));
        assertThat(events.get(2), is(event_3));
    }

    @Test
    public void shouldInsertIntoEventLogTableUsingParameters() throws Exception {

        new TableCleaner().clean("event_log", eventStoreDataSource);

        assertThat(eventStoreDataAccess.findAllEvents(), is(emptyList()));

        final Event event_1 = eventBuilder().withName("event 1").build();
        final Event event_2 = eventBuilder().withName("event 2").build();
        final Event event_3 = eventBuilder().withName("event 3").build();

        eventStoreDataAccess.insertIntoEventLog(
                event_1.getId(),
                event_1.getStreamId(),
                event_1.getPositionInStream(),
                event_1.getCreatedAt(),
                event_1.getName(),
                event_1.getPayload(),
                event_1.getMetadata()
        );
        eventStoreDataAccess.insertIntoEventLog(
                event_2.getId(),
                event_2.getStreamId(),
                event_2.getPositionInStream(),
                event_2.getCreatedAt(),
                event_2.getName(),
                event_2.getPayload(),
                event_2.getMetadata()
        );
        eventStoreDataAccess.insertIntoEventLog(
                event_3.getId(),
                event_3.getStreamId(),
                event_3.getPositionInStream(),
                event_3.getCreatedAt(),
                event_3.getName(),
                event_3.getPayload(),
                event_3.getMetadata()
        );


        final List<Event> events = eventStoreDataAccess.findAllEvents();

        assertThat(events.size(), is(3));

        assertThat(events.get(0), is(event_1));
        assertThat(events.get(1), is(event_2));
        assertThat(events.get(2), is(event_3));
    }

    @Test
    public void shouldGetEventsByStreamId() throws Exception {

        new TableCleaner().clean("event_log", eventStoreDataSource);

        assertThat(eventStoreDataAccess.findAllEvents(), is(emptyList()));

        final UUID streamId_1 = UUID.randomUUID();
        final UUID streamId_2 = UUID.randomUUID();

        final Event event_1 = eventBuilder().withName("event 1").withStreamId(streamId_1).withPositionInStream(1L).build();
        final Event event_2 = eventBuilder().withName("event 2").withStreamId(streamId_2).withPositionInStream(1L).build();
        final Event event_3 = eventBuilder().withName("event 3").withStreamId(streamId_1).withPositionInStream(2L).build();

        eventStoreDataAccess.insertIntoEventLog(event_1);
        eventStoreDataAccess.insertIntoEventLog(event_2);
        eventStoreDataAccess.insertIntoEventLog(event_3);

        final List<Event> eventsOfStream_1 = eventStoreDataAccess.findEventsByStream(streamId_1);

        assertThat(eventsOfStream_1.size(), is(2));

        assertThat(eventsOfStream_1.get(0), is(event_1));
        assertThat(eventsOfStream_1.get(1), is(event_3));

        final List<Event> eventsOfStream_2 = eventStoreDataAccess.findEventsByStream(streamId_2);

        assertThat(eventsOfStream_2.size(), is(1));
        assertThat(eventsOfStream_2.get(0), is(event_2));
    }

    @Test
    public void shouldInsertAndGetPublishedEvents() throws Exception {

        new TableCleaner().clean("published_event", eventStoreDataSource);

        assertThat(eventStoreDataAccess.findAllPublishedEvents(), is(emptyList()));

        final LinkedEvent linkedEvent_1 = linkedEventBuilder()
                .withName("published event 1")
                .withPreviousEventNumber(0)
                .withEventNumber(1)
                .build();
        final LinkedEvent linkedEvent_2 = linkedEventBuilder()
                .withName("published event 2")
                .withPreviousEventNumber(1)
                .withEventNumber(2)
                .build();
        final LinkedEvent linkedEvent_3 = linkedEventBuilder()
                .withName("published event 3")
                .withPreviousEventNumber(2)
                .withEventNumber(3)
                .build();

        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_1);
        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_2);
        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_3);

        final List<LinkedEvent> linkedEvents = eventStoreDataAccess.findAllPublishedEvents();

        assertThat(linkedEvents.size(), is(3));
        assertThat(linkedEvents.get(0), is(linkedEvent_1));
        assertThat(linkedEvents.get(1), is(linkedEvent_2));
        assertThat(linkedEvents.get(2), is(linkedEvent_3));
    }

    @Test
    public void shouldGetPublishedEventsByOrderedByEventNumber() throws Exception {

        new TableCleaner().clean("published_event", eventStoreDataSource);

        assertThat(eventStoreDataAccess.findAllPublishedEvents(), is(emptyList()));

        final LinkedEvent linkedEvent_1 = linkedEventBuilder()
                .withName("published event 1")
                .withPreviousEventNumber(0)
                .withEventNumber(1)
                .build();
        final LinkedEvent linkedEvent_2 = linkedEventBuilder()
                .withName("published event 2")
                .withPreviousEventNumber(1)
                .withEventNumber(2)
                .build();
        final LinkedEvent linkedEvent_3 = linkedEventBuilder()
                .withName("published event 3")
                .withPreviousEventNumber(2)
                .withEventNumber(3)
                .build();

        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_3);
        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_1);
        eventStoreDataAccess.insertIntoPublishedEvent(linkedEvent_2);

        final List<LinkedEvent> linkedEvents = eventStoreDataAccess.findAllPublishedEventsOrderedByEventNumber();

        assertThat(linkedEvents.size(), is(3));
        assertThat(linkedEvents.get(0), is(linkedEvent_1));
        assertThat(linkedEvents.get(1), is(linkedEvent_2));
        assertThat(linkedEvents.get(2), is(linkedEvent_3));
    }
}
