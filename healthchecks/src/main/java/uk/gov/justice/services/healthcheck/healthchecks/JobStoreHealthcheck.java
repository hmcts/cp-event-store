package uk.gov.justice.services.healthcheck.healthchecks;

import static java.lang.String.format;
import static java.util.List.of;
import static uk.gov.justice.services.healthcheck.api.HealthcheckResult.failure;

import uk.gov.justice.framework.libraries.datasource.providers.jobstore.JndiJobStoreDataSourceProvider;
import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;
import uk.gov.justice.services.healthcheck.utils.database.TableChecker;

import java.sql.SQLException;
import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class JobStoreHealthcheck implements Healthcheck {

    public static final String JOB_STORE_HEALTHCHECK_NAME = "job-store-healthcheck";

    public static final List<String> JOB_STORE_TABLE_NAMES = of("job");

    @Inject
    private JndiJobStoreDataSourceProvider jndiJobStoreDataSourceProvider;

    @Inject
    private TableChecker tableChecker;

    @Inject
    private Logger logger;

    @Override
    public String getHealthcheckName() {
        return JOB_STORE_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the jobstore database and that all tables are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {

        final DataSource jobStoreDataSource = jndiJobStoreDataSourceProvider.getJobStoreDataSource();

        try {
            return tableChecker.checkTables(JOB_STORE_TABLE_NAMES, jobStoreDataSource);

        } catch (final SQLException e) {
            logger.error("Healthcheck for jobstore database failed.", e);
            return failure(format("Exception thrown accessing jobstore database. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
