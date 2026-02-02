package uk.gov.justice.services.test.utils.persistence;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TableCleanerTest {

    @InjectMocks
    private TableCleaner tableCleaner;

    @Test
    public void shouldTruncateDatabaseTable() throws Exception {

        final String tableName = "event_log";
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("TRUNCATE event_log")).thenReturn(preparedStatement);

        tableCleaner.clean(tableName, dataSource);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(connection).close();
    }
}
