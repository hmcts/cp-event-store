package uk.gov.justice.services.test.utils.persistence;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import jakarta.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;

@ApplicationScoped
public class SettableEventStoreDataSourceProvider implements EventStoreDataSourceProvider {

    private DataSource dataSource;

    @Override
    public DataSource getDefaultDataSource() {
        return dataSource;
    }

    public void setDataSource(final DataSource dataSource) {

        this.dataSource = dataSource;
    }

    @Override
    public DataSource getDataSource(final String jndiName) {
        return dataSource;
    }
}
