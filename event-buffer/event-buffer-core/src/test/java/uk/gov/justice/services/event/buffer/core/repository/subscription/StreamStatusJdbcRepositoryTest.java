package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamStatusJdbcRepositoryTest {

    public static final String EVENT_LISTENER = "EVENT_LISTENER";

    @SuppressWarnings("unused")
    @Spy
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory = new PreparedStatementWrapperFactory();

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;
    
    @Mock
    private UtcClock clock;

    @InjectMocks
    private StreamStatusJdbcRepository streamStatusJdbcRepository;

    @Test
    public void shouldAttemptToInsert() throws Exception {

        final String source = "a source";
        final ZonedDateTime now = new UtcClock().now();
        final UUID streamId = randomUUID();
        final long position = 1l;
        final String component = EVENT_LISTENER;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(clock.now()).thenReturn(now);
        when(connection.prepareStatement("INSERT INTO stream_status (position, stream_id, source, component, discovered_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"))
                .thenReturn(preparedStatement);

        streamStatusJdbcRepository.insertOrDoNothing(new Subscription(streamId, position, source, component));

        verify(preparedStatement).setLong(1, position);
        verify(preparedStatement).setObject(2, streamId);
        verify(preparedStatement).setString(3, source);
        verify(preparedStatement).setString(4, component);
        verify(preparedStatement).setTimestamp(5, toSqlTimestamp(now));
        verify(preparedStatement).executeUpdate();
    }
}
