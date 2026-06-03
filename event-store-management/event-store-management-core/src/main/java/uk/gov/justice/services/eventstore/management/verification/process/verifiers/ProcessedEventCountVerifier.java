package uk.gov.justice.services.eventstore.management.verification.process.verifiers;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.error;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.success;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.eventstore.management.verification.process.TableRowCounter;
import uk.gov.justice.services.eventstore.management.verification.process.VerificationResult;
import uk.gov.justice.services.eventstore.management.verification.process.Verifier;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class ProcessedEventCountVerifier implements Verifier {

    private static final String PUBLISHED_EVENT_TABLE_NAME = "published_event";
    private static final String PROCESSED_EVENT_TABLE_NAME = "processed_event";

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private TableRowCounter tableRowCounter;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying that the number of events in processed_event matches the number of events in published_event...");

        final DataSource eventStoreDataSource = eventStoreDataSourceProvider.getDefaultDataSource();
        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();

        final int publishedEventCount = tableRowCounter.countRowsIn(
                PUBLISHED_EVENT_TABLE_NAME,
                eventStoreDataSource);

        final int processedEventCount = tableRowCounter.countRowsIn(
                PROCESSED_EVENT_TABLE_NAME,
                viewStoreDataSource);

        if (publishedEventCount != processedEventCount) {

            final String message =
                    "The number of events in processed_event " +
                            "does not match the number of events in published event. " +
                            "published_event: %d, " +
                            "processed_event: %d";

            return singletonList(error(format(message, publishedEventCount, processedEventCount)));
        }

        if (publishedEventCount == 0) {
            return singletonList(error("published_event and processed_event both contain zero events"));
        }

        return singletonList(success(format("published_event and processed_event both contain %d events", processedEventCount)));
    }
}
