package uk.gov.justice.services.eventstore.management.verification.process;

import static java.util.Arrays.asList;

import uk.gov.justice.services.eventstore.management.commands.VerificationCommand;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.ProcessedEventCountVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.ProcessedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.PublishedEventCountVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.PublishedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.StreamBufferEmptyVerifier;

import java.util.List;

import jakarta.inject.Inject;

public class VerifierProvider {

    @Inject
    private AllEventsInStreamsVerifier allEventsInStreamsVerifier;

    @Inject
    private ProcessedEventCountVerifier processedEventCountVerifier;

    @Inject
    private ProcessedEventLinkVerifier processedEventLinkVerifier;

    @Inject
    private PublishedEventCountVerifier publishedEventCountVerifier;

    @Inject
    private PublishedEventLinkVerifier publishedEventLinkVerifier;

    @Inject
    private StreamBufferEmptyVerifier streamBufferEmptyVerifier;

    public List<Verifier> getVerifiers(final VerificationCommand verificationCommand) {

        if (verificationCommand.isCatchupVerification()) {
            return getCatchupVerifiers();
        }

        return getRebuildVerifiers();
    }

    private List<Verifier> getRebuildVerifiers() {
        return asList(
                publishedEventCountVerifier,
                publishedEventLinkVerifier,
                allEventsInStreamsVerifier
        );
    }

    private List<Verifier> getCatchupVerifiers() {
        return asList(
                streamBufferEmptyVerifier,
                publishedEventCountVerifier,
                processedEventCountVerifier,
                publishedEventLinkVerifier,
                processedEventLinkVerifier,
                allEventsInStreamsVerifier
        );
    }
}
