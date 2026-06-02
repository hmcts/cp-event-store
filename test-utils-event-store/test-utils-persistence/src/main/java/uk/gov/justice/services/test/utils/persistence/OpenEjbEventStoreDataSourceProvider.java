package uk.gov.justice.services.test.utils.persistence;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;

@ApplicationScoped
public class OpenEjbEventStoreDataSourceProvider implements EventStoreDataSourceProvider {

    @Resource(name = "openejb/Resource/frameworkeventstore")
    private DataSource dataSource;

    @Override
    public DataSource getDefaultDataSource() {
        return dataSource;
    }

    @Override
    public DataSource getDataSource(final String jndiName) {
        return dataSource;
    }
}
