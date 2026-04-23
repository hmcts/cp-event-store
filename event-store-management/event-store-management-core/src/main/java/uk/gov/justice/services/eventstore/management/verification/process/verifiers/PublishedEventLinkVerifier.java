package uk.gov.justice.services.eventstore.management.verification.process.verifiers;

import static uk.gov.justice.services.eventstore.management.verification.process.LinkedEventNumberTable.EVENT_LOG;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.eventstore.management.verification.process.EventLinkageChecker;
import uk.gov.justice.services.eventstore.management.verification.process.VerificationResult;
import uk.gov.justice.services.eventstore.management.verification.process.Verifier;

import java.util.List;

import jakarta.inject.Inject;

import org.slf4j.Logger;

public class PublishedEventLinkVerifier implements Verifier {

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private EventLinkageChecker eventLinkageChecker;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying all previous_event_numbers in processed_event point to an existing event...");

        return eventLinkageChecker.verifyEventNumbersAreLinkedCorrectly(
                EVENT_LOG,
                eventStoreDataSourceProvider.getDefaultDataSource());
    }
}
