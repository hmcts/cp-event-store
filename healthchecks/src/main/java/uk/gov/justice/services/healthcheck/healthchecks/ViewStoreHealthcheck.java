package uk.gov.justice.services.healthcheck.healthchecks;

import static java.lang.String.format;
import static java.util.List.of;
import static uk.gov.justice.services.healthcheck.api.HealthcheckResult.failure;

import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;
import uk.gov.justice.services.healthcheck.utils.database.TableChecker;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.SQLException;
import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class ViewStoreHealthcheck implements Healthcheck {

    public static final String VIEW_STORE_HEALTHCHECK_NAME = "view-store-healthcheck";

    public static final List<String> VIEW_STORE_TABLE_NAMES = of(
            "stream_buffer",
            "stream_status",
            "processed_event");

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private TableChecker tableChecker;

    @Inject
    private Logger logger;

    @Override
    public String getHealthcheckName() {
        return VIEW_STORE_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the viewstore database and that all framework tables are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {
        final DataSource eventStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();

        try {
            return tableChecker.checkTables(VIEW_STORE_TABLE_NAMES, eventStoreDataSource);

        } catch (final SQLException e) {
            logger.error("Healthcheck for viewstore database failed.", e);
            return failure(format("Exception thrown accessing viewstore database. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
