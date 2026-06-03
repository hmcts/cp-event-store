package uk.gov.justice.services.healthcheck.healthchecks;

import static java.lang.String.format;
import static java.util.List.of;
import static uk.gov.justice.services.healthcheck.api.HealthcheckResult.failure;

import uk.gov.justice.services.fileservice.api.FileServiceException;
import uk.gov.justice.services.fileservice.repository.DataSourceProvider;
import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;
import uk.gov.justice.services.healthcheck.utils.database.TableChecker;

import java.sql.SQLException;
import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class FileStoreHealthcheck implements Healthcheck {

    public static final String FILE_STORE_HEALTHCHECK_NAME = "file-store-healthcheck";

    public static final List<String> FILE_STORE_TABLE_NAMES = of("content", "metadata");

    @Inject
    private DataSourceProvider fileStoreDataSourceProvider;

    @Inject
    private TableChecker tableChecker;

    @Inject
    private Logger logger;

    @Override
    public String getHealthcheckName() {
        return FILE_STORE_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the filestore database and that all tables are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {


        try {
            final DataSource jobStoreDataSource = fileStoreDataSourceProvider.getDatasource();
            return tableChecker.checkTables(FILE_STORE_TABLE_NAMES, jobStoreDataSource);

        } catch (final SQLException | FileServiceException e) {
            logger.error("Healthcheck for filestore database failed.", e);
            return failure(format("Exception thrown accessing filestore database. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
