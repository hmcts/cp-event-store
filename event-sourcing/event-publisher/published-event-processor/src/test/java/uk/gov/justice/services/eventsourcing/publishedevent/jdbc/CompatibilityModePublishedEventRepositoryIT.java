package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.JsonValue.NULL;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import javax.json.JsonValue;
import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CompatibilityModePublishedEventRepositoryIT {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @BeforeEach
    public void cleanEventLogTables() throws Exception {
        new DatabaseCleaner().cleanEventStoreTables("framework");
    }

    @Test
    public void shouldInsertEnvelopeIntoPublishedEventTable() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String name = "some-event-name";
        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;
        final long positionInStream = 234L;
        final String payloadAsJson = "some-payload-json";
        final ZonedDateTime createdAt = new UtcClock().now();

        final Metadata metadataWithEventNumbers = metadataBuilder()
                .withId(eventId)
                .withName(name)
                .withStreamId(streamId)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .createdAt(createdAt)
                .withPosition(positionInStream)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .build();

        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);
        final JsonValue payload = mock(JsonValue.class);

        when(linkedJsonEnvelope.metadata()).thenReturn(metadataWithEventNumbers);
        when(linkedJsonEnvelope.payload()).thenReturn(payload);
        when(payload.toString()).thenReturn(payloadAsJson);

        compatibilityModePublishedEventRepository.insertIntoPublishedEvent(linkedJsonEnvelope);

        final List<LinkedEvent> foundEvents = compatibilityModePublishedEventRepository.findAll();

        assertThat(foundEvents.size(), is(1));
        assertThat(foundEvents.get(0).getId(), is(eventId));
        assertThat(foundEvents.get(0).getStreamId(), is(streamId));
        assertThat(foundEvents.get(0).getPositionInStream(), is(positionInStream));
        assertThat(foundEvents.get(0).getName(), is(name));
        assertThat(foundEvents.get(0).getMetadata(), is(metadataWithEventNumbers.asJsonObject().toString()));
        assertThat(foundEvents.get(0).getPayload(), is(payloadAsJson));
        assertThat(foundEvents.get(0).getCreatedAt(), is(createdAt));
        assertThat(foundEvents.get(0).getEventNumber(), is(of(eventNumber)));
        assertThat(foundEvents.get(0).getPreviousEventNumber(), is(previousEventNumber));
    }

    @Test
    public void shouldHandleJsonNullPayload() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String name = "some-event-name";
        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;
        final long positionInStream = 234L;
        final ZonedDateTime createdAt = new UtcClock().now();

        final Metadata metadataWithEventNumbers = metadataBuilder()
                .withId(eventId)
                .withName(name)
                .withStreamId(streamId)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .createdAt(createdAt)
                .withPosition(positionInStream)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .build();

        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);

        when(linkedJsonEnvelope.metadata()).thenReturn(metadataWithEventNumbers);
        when(linkedJsonEnvelope.payload()).thenReturn(NULL);

        compatibilityModePublishedEventRepository.insertIntoPublishedEvent(linkedJsonEnvelope);

        final List<LinkedEvent> foundEvents = compatibilityModePublishedEventRepository.findAll();

        assertThat(foundEvents.size(), is(1));
        assertThat(foundEvents.get(0).getId(), is(eventId));
        assertThat(foundEvents.get(0).getStreamId(), is(streamId));
        assertThat(foundEvents.get(0).getPositionInStream(), is(positionInStream));
        assertThat(foundEvents.get(0).getName(), is(name));
        assertThat(foundEvents.get(0).getMetadata(), is(metadataWithEventNumbers.asJsonObject().toString()));
        assertThat(foundEvents.get(0).getPayload(), is("null"));
        assertThat(foundEvents.get(0).getCreatedAt(), is(createdAt));
        assertThat(foundEvents.get(0).getEventNumber(), is(of(eventNumber)));
        assertThat(foundEvents.get(0).getPreviousEventNumber(), is(previousEventNumber));
    }

    @Test
    public void shouldName() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);


        final Long currentSequenceNumber = getCurrentSequenceNumber();
        final Long newSequenceNumber = currentSequenceNumber + 23;

        compatibilityModePublishedEventRepository.setEventNumberSequenceTo(newSequenceNumber);

        assertThat(getCurrentSequenceNumber(), is(newSequenceNumber));

        compatibilityModePublishedEventRepository.setEventNumberSequenceTo(currentSequenceNumber);
        assertThat(getCurrentSequenceNumber(), is(currentSequenceNumber));

    }

    private Long getCurrentSequenceNumber() throws SQLException {

        final String sql = """
                SELECT last_value FROM event_sequence_seq;
                """;
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        try(final Connection connection = eventStoreDataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(sql);
            final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

            throw new RuntimeException("Failed to get last value from 'event_sequence_seq' sequence");
        }
    }
}