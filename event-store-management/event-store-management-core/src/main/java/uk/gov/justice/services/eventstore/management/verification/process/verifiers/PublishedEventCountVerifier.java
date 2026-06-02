package uk.gov.justice.services.eventstore.management.verification.process.verifiers;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.error;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.success;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.eventstore.management.verification.process.EventLogActiveEventRowCounter;
import uk.gov.justice.services.eventstore.management.verification.process.TableRowCounter;
import uk.gov.justice.services.eventstore.management.verification.process.VerificationResult;
import uk.gov.justice.services.eventstore.management.verification.process.Verifier;

import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class PublishedEventCountVerifier implements Verifier {

    private static final String PUBLISHED_EVENT_TABLE_NAME = "published_event";

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private EventLogActiveEventRowCounter eventLogActiveEventRowCounter;

    @Inject
    private TableRowCounter tableRowCounter;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying that the number of active events in event_log matches the number of events in processed_event...");

        final DataSource eventStoreDataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        final int eventLogCount = eventLogActiveEventRowCounter.getActiveEventCountFromEventLog(
                eventStoreDataSource);

        final int publishedEventCount = tableRowCounter.countRowsIn(
                PUBLISHED_EVENT_TABLE_NAME,
                eventStoreDataSource);

        if (eventLogCount != publishedEventCount) {

            final String message =
                    "The number of active events in event_log " +
                            "does not match the number of events in published event. " +
                            "event_log: %d, " +
                            "published: %d";

            return singletonList(error(format(message, eventLogCount, publishedEventCount)));
        }

        if (publishedEventCount == 0) {
            return singletonList(error("The tables event_log and published both contain zero active events"));
        }

        return singletonList(success(format("The tables event_log and published_event both contain %d active events", eventLogCount)));
    }
}
