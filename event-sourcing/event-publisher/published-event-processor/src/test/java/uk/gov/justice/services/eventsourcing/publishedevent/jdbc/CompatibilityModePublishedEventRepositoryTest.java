package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository.INSERT_INTO_PUBLISHED_EVENT_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository.SET_EVENT_NUMBER_SEQUENCE_SQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CompatibilityModePublishedEventRepositoryTest {

    @InjectMocks
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @Test
    public void shouldInsertBatchIntoPublishedEvent() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenReturn(preparedStatement);

        final List<LinkedEventData> linkDataList = List.of(
                new LinkedEventData(eventId1, 1L, 0L),
                new LinkedEventData(eventId2, 2L, 1L)
        );

        compatibilityModePublishedEventRepository.insertBatchIntoPublishedEvent(connection, linkDataList);

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setLong(1, 1L);
        inOrder.verify(preparedStatement).setLong(2, 0L);
        inOrder.verify(preparedStatement).setObject(3, eventId1);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).setLong(1, 2L);
        inOrder.verify(preparedStatement).setLong(2, 1L);
        inOrder.verify(preparedStatement).setObject(3, eventId2);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).executeBatch();
    }

    @Test
    public void shouldSetEventNumberSequenceUsingProvidedConnection() throws Exception {

        final long eventNumber = 42L;
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(SET_EVENT_NUMBER_SEQUENCE_SQL)).thenReturn(preparedStatement);

        compatibilityModePublishedEventRepository.setEventNumberSequenceTo(connection, eventNumber);

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setLong(1, eventNumber);
        inOrder.verify(preparedStatement).execute();
    }
}