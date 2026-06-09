package uk.gov.justice.services.test.utils.persistence;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;


public class FrameworkTestDataSourceFactoryTest {

    private final FrameworkTestDataSourceFactory frameworkTestDataSourceFactory = new FrameworkTestDataSourceFactory();

    @Test
    public void shouldGetADataSourceToTheEventStore() throws Exception {

        final DataSource eventStoreDataSource = frameworkTestDataSourceFactory.createEventStoreDataSource();

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            assertThat(connection.getCatalog(), is("frameworkeventstore"));
        }
    }

    @Test
    public void shouldGetADataSourceToTheViewStore() throws Exception {

        final DataSource viewStoreDataSource = frameworkTestDataSourceFactory.createViewStoreDataSource();

        try (final Connection connection = viewStoreDataSource.getConnection()) {
            assertThat(connection.getCatalog(), is("frameworkviewstore"));
        }
    }

    @Test
    public void shouldGetADataSourceToTheFileStore() throws Exception {

        final DataSource fileStoreDataSource = frameworkTestDataSourceFactory.createFileStoreDataSource();

        try (final Connection connection = fileStoreDataSource.getConnection()) {
            assertThat(connection.getCatalog(), is("frameworkfilestore"));
        }
    }

    @Test
    public void shouldLoadPropertiesSuccessfully(){
        final Properties prop = frameworkTestDataSourceFactory.getTestDatSourceProperties();

        MatcherAssert.assertThat(prop.getProperty("PORT_NUMBER"), is("5432"));
        MatcherAssert.assertThat(prop.getProperty("USERNAME"), is("framework"));
        MatcherAssert.assertThat(prop.getProperty("PASSWORD"), is("framework"));
    }
}
