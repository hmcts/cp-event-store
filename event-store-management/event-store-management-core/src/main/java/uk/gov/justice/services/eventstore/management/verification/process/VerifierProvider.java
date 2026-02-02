package uk.gov.justice.services.eventstore.management.verification.process;

import java.util.List;
import javax.inject.Inject;
import uk.gov.justice.services.eventstore.management.commands.VerificationCommand;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.ProcessedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.PublishedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.StreamBufferEmptyVerifier;

import static java.util.Arrays.asList;

public class VerifierProvider {

    @Inject
    private AllEventsInStreamsVerifier allEventsInStreamsVerifier;

    @Inject
    private ProcessedEventLinkVerifier processedEventLinkVerifier;

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
                publishedEventLinkVerifier,
                allEventsInStreamsVerifier
        );
    }

    private List<Verifier> getCatchupVerifiers() {
        return asList(
                streamBufferEmptyVerifier,
                publishedEventLinkVerifier,
                processedEventLinkVerifier,
                allEventsInStreamsVerifier
        );
    }
}
