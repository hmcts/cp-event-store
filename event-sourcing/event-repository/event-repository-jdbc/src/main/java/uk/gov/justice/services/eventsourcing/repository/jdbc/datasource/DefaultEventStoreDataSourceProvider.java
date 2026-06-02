package uk.gov.justice.services.eventsourcing.repository.jdbc.datasource;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.jdbc.persistence.JdbcDataSourceProvider;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import jakarta.ejb.Singleton;
import jakarta.inject.Inject;
import javax.sql.DataSource;

@Singleton
public class DefaultEventStoreDataSourceProvider implements EventStoreDataSourceProvider {

    private DataSource dataSource;

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private JdbcDataSourceProvider jdbcDataSourceProvider;

    @Override
    public synchronized DataSource getDefaultDataSource() {

        if (null == dataSource) {
            dataSource = getDataSourceFromJndi();
        }

        return dataSource;
    }

    @Override
    public DataSource getDataSource(final String jndiName) {
        return jdbcDataSourceProvider.getDataSource(jndiName);
    }

    private DataSource getDataSourceFromJndi() {
        final EventSourceDefinition defaultEventSourceDefinition = eventSourceDefinitionRegistry.getDefaultEventSourceDefinition();

        final String jndiDatasource = defaultEventSourceDefinition
                .getLocation()
                .getDataSource()
                .orElseThrow(() -> new MissingEventStoreDataSourceName("No data_source specified in 'event-sources.yaml' marked as default"));

        return jdbcDataSourceProvider.getDataSource(jndiDatasource);
    }
}
