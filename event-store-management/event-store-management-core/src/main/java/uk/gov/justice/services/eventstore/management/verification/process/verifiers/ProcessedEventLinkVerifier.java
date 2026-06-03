package uk.gov.justice.services.eventstore.management.verification.process.verifiers;

import static uk.gov.justice.services.eventstore.management.verification.process.LinkedEventNumberTable.PROCESSED_EVENT;

import uk.gov.justice.services.eventstore.management.verification.process.EventLinkageChecker;
import uk.gov.justice.services.eventstore.management.verification.process.VerificationResult;
import uk.gov.justice.services.eventstore.management.verification.process.Verifier;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.util.List;

import jakarta.inject.Inject;

import org.slf4j.Logger;

public class ProcessedEventLinkVerifier implements Verifier {

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private EventLinkageChecker eventLinkageChecker;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying all previous_event_numbers in processed_event point to an existing event...");

        return eventLinkageChecker.verifyEventNumbersAreLinkedCorrectly(
                PROCESSED_EVENT,
                viewStoreJdbcDataSourceProvider.getDataSource());
    }
}
