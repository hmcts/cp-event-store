package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

@ApplicationScoped
public class EventBufferJdbcRepository {

    private static final String INSERT = "INSERT INTO stream_buffer (stream_id, position, event, source, component, buffered_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
    private static final String SELECT_STREAM_BUFFER_BY_STREAM_ID_SOURCE_AND_COMPONENT = "SELECT stream_id, position, event, source, component, buffered_at FROM stream_buffer WHERE stream_id=? AND source=? AND component=? ORDER BY position";
    private static final String DELETE_BY_STREAM_ID_POSITION = "DELETE FROM stream_buffer WHERE stream_id=? AND position=? AND source=? AND component=?";

    private static final String STREAM_ID = "stream_id";
    private static final String POSITION = "position";
    private static final String EVENT = "event";
    private static final String SOURCE = "source";
    private static final String COMPONENT = "component";
    private static final String BUFFERED_AT = "buffered_at";

    @Inject
    private JdbcResultSetStreamer jdbcResultSetStreamer;

    @Inject
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory;

    @Inject
    private ViewStoreJdbcDataSourceProvider dataSourceProvider;

    @Inject
    private Logger logger;

    private DataSource dataSource;

    public EventBufferJdbcRepository() {
    }

    public EventBufferJdbcRepository(final JdbcResultSetStreamer jdbcResultSetStreamer,
                                     final PreparedStatementWrapperFactory preparedStatementWrapperFactory,
                                     final DataSource dataSource,
                                     final Logger logger) {
        this.jdbcResultSetStreamer = jdbcResultSetStreamer;
        this.dataSource = dataSource;
        this.preparedStatementWrapperFactory = preparedStatementWrapperFactory;
        this.logger = logger;
    }

    @PostConstruct
    private void initialiseDataSource() {
        dataSource = dataSourceProvider.getDataSource();
    }

    public void insert(final EventBufferEvent bufferedEvent) {
        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, INSERT)) {
            ps.setObject(1, bufferedEvent.getStreamId());
            ps.setLong(2, bufferedEvent.getPosition());
            ps.setString(3, bufferedEvent.getEvent());
            ps.setString(4, bufferedEvent.getSource());
            ps.setString(5, bufferedEvent.getComponent());
            ps.setTimestamp(6, toSqlTimestamp(bufferedEvent.getBufferedAt()));
            final int rowsUpdated = ps.executeUpdate();
            if (rowsUpdated == 0) {
                logger.warn("Event already present in event buffer. Ignoring");
            }

        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while storing event in the buffer: %s", bufferedEvent), e);
        }
    }

    public Stream<EventBufferEvent> findStreamByIdSourceAndComponent(final UUID id, final String source, final String component) {
        try {
            final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SELECT_STREAM_BUFFER_BY_STREAM_ID_SOURCE_AND_COMPONENT);
            ps.setObject(1, id);
            ps.setString(2, source);
            ps.setString(3, component);

            return jdbcResultSetStreamer.streamOf(ps, entityFromFunction());

        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while returning buffered events, streamId: %s", id), e);
        }
    }

    public void remove(final EventBufferEvent eventBufferEvent) {

        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, DELETE_BY_STREAM_ID_POSITION)) {
            ps.setObject(1, eventBufferEvent.getStreamId());
            ps.setLong(2, eventBufferEvent.getPosition());
            ps.setString(3, eventBufferEvent.getSource());
            ps.setString(4, eventBufferEvent.getComponent());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while removing event from the buffer: %s", eventBufferEvent), e);
        }
    }

    private Function<ResultSet, EventBufferEvent> entityFromFunction() {
        return resultSet -> {
            try {
                return new EventBufferEvent((UUID) resultSet.getObject(STREAM_ID),
                        // position column is nullable, Use Long Object to prevent jdbc
                        // converting nulls to zero
                        resultSet.getObject(POSITION, Long.class),
                        resultSet.getString(EVENT),
                        resultSet.getString(SOURCE),
                        resultSet.getString(COMPONENT),
                        fromSqlTimestamp(resultSet.getTimestamp(BUFFERED_AT))
                );
            } catch (final SQLException e) {
                throw new JdbcRepositoryException("Unexpected SQLException mapping ResultSet to StreamBufferEntity instance", e);
            }
        };
    }
}
