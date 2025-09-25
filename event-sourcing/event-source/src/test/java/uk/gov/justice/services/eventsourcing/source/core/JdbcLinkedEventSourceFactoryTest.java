package uk.gov.justice.services.eventsourcing.source.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourceEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourcePublishedEventRepositoryFactory;
import uk.gov.justice.services.jdbc.persistence.JdbcDataSourceProvider;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JdbcLinkedEventSourceFactoryTest {

    @Mock
    private MultipleDataSourcePublishedEventRepositoryFactory multipleDataSourcePublishedEventRepositoryFactory;

    @Mock
    private JdbcDataSourceProvider jdbcDataSourceProvider;

    @InjectMocks
    private JdbcLinkedEventSourceFactory jdbcLinkedEventSourceFactory;

    @Test
    public void shouldCreateJdbcBasedPublishedEventSource() throws Exception {

        final String jndiDatasource = "jndiDatasource";

        final DataSource dataSource = jdbcDataSourceProvider.getDataSource(jndiDatasource);
        final MultipleDataSourceEventRepository multipleDataSourceEventRepository = multipleDataSourcePublishedEventRepositoryFactory.create(dataSource);

        when(jdbcDataSourceProvider.getDataSource(jndiDatasource)).thenReturn(dataSource);
        when(multipleDataSourcePublishedEventRepositoryFactory.create(dataSource)).thenReturn(multipleDataSourceEventRepository);

        final DefaultLinkedEventSource defaultPublishedEventSource = jdbcLinkedEventSourceFactory.create(jndiDatasource);

        assertThat(getValueOfField(defaultPublishedEventSource, "multipleDataSourceEventRepository", MultipleDataSourceEventRepository.class), is(multipleDataSourceEventRepository));
    }
}
