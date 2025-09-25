package uk.gov.justice.services.eventsourcing.source.core;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourceEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourcePublishedEventRepositoryFactory;
import uk.gov.justice.services.jdbc.persistence.JdbcDataSourceProvider;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;

@ApplicationScoped
public class JdbcLinkedEventSourceFactory {

    @Inject
    private MultipleDataSourcePublishedEventRepositoryFactory multipleDataSourcePublishedEventRepositoryFactory;

    @Inject
    private JdbcDataSourceProvider jdbcDataSourceProvider;

    public DefaultLinkedEventSource create(final String jndiDatasource) {

        final DataSource dataSource = jdbcDataSourceProvider.getDataSource(jndiDatasource);
        final MultipleDataSourceEventRepository multipleDataSourceEventRepository = multipleDataSourcePublishedEventRepositoryFactory.create(dataSource);

        return new DefaultLinkedEventSource(multipleDataSourceEventRepository);
    }
}
