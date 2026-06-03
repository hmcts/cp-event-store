package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;

import jakarta.inject.Inject;
import javax.sql.DataSource;

public class MultipleDataSourcePublishedEventRepositoryFactory {

    @Inject
    private JdbcResultSetStreamer jdbcResultSetStreamer;

    @Inject
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory;

    public MultipleDataSourceEventRepository create(final DataSource dataSource) {
        return new MultipleDataSourceEventRepository(
                jdbcResultSetStreamer,
                preparedStatementWrapperFactory,
                dataSource);
    }
}
