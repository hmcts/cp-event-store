package uk.gov.justice.services.healthcheck.healthchecks;

import static java.lang.String.format;
import static java.util.List.of;
import static uk.gov.justice.services.healthcheck.api.HealthcheckResult.failure;

import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;
import uk.gov.justice.services.healthcheck.utils.database.TableChecker;
import uk.gov.justice.services.jdbc.persistence.SystemJdbcDataSourceProvider;

import java.sql.SQLException;
import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class SystemDatabaseHealthcheck implements Healthcheck {

    public static final String SYSTEM_DATABASE_HEALTHCHECK_NAME = "system-database-healthcheck";

    public static final List<String> SYSTEM_DATABASE_TABLE_NAMES = of(
            "system_command_status",
            "stored_command");

    @Inject
    private SystemJdbcDataSourceProvider systemJdbcDataSourceProvider;

    @Inject
    private TableChecker tableChecker;

    @Inject
    private Logger logger;

    @Override
    public String getHealthcheckName() {
        return SYSTEM_DATABASE_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the system database and that all tables are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {

        final DataSource jobStoreDataSource = systemJdbcDataSourceProvider.getDataSource();

        try {
            return tableChecker.checkTables(SYSTEM_DATABASE_TABLE_NAMES, jobStoreDataSource);

        } catch (final SQLException e) {
            logger.error("Healthcheck for system database failed.", e);
            return failure(format("Exception thrown accessing system database. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
