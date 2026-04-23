package uk.gov.justice.services.eventstore.management.verification.process;

import static java.lang.String.format;

import java.util.List;

import jakarta.inject.Inject;

import org.slf4j.Logger;

public class VerificationResultsLogger {

    @Inject
    private Logger logger;

    public void logResults(
            final List<VerificationResult> successfulResults,
            final List<VerificationResult> warningResults,
            final List<VerificationResult> errorResults) {

        logger.info(format("Verification of Catchup completed with %d Errors, %d Warnings and %d Successes", errorResults.size(), warningResults.size(), successfulResults.size()));

        errorResults.forEach(verificationResult -> logger.error("ERROR: " + verificationResult.getMessage()));
        warningResults.forEach(verificationResult -> logger.warn("WARNING: " + verificationResult.getMessage()));
        successfulResults.forEach(verificationResult -> logger.info("SUCCESS: " + verificationResult.getMessage()));
    }
}
