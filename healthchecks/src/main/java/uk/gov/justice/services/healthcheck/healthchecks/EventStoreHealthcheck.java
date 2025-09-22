package uk.gov.justice.services.healthcheck.healthchecks;

import static java.lang.String.format;
import static java.util.List.of;
import static uk.gov.justice.services.healthcheck.api.HealthcheckResult.failure;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;
import uk.gov.justice.services.healthcheck.utils.database.TableChecker;

import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class EventStoreHealthcheck implements Healthcheck {

    public static final String EVENT_STORE_HEALTHCHECK_NAME = "event-store-healthcheck";

    public static final List<String> EVENT_STORE_TABLE_NAMES = of(
            "event_stream",
            "event_log",
            "published_event",
            "publish_queue",
            "snapshot");

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private TableChecker tableChecker;

    @Inject
    private Logger logger;

    @Override
    public String getHealthcheckName() {
        return EVENT_STORE_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the eventstore database and that all framework tables are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {

        final DataSource eventStoreDataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            return tableChecker.checkTables(EVENT_STORE_TABLE_NAMES, eventStoreDataSource);

        } catch (final SQLException e) {
            logger.error("Healthcheck for eventstore database failed.", e);
            return failure(format("Exception thrown accessing eventstore database. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
