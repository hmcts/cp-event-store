package uk.gov.justice.services.eventstore.management.verification.process;

import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventstore.management.commands.VerificationCommand;

import java.util.Collection;
import java.util.List;

import jakarta.inject.Inject;

public class VerificationRunner {

    @Inject
    private VerifierProvider verifierProvider;

    public List<VerificationResult> runVerifiers(final VerificationCommand verificationCommand) {

        return verifierProvider.getVerifiers(verificationCommand).stream()
                .map(Verifier::verify)
                .flatMap(Collection::stream)
                .collect(toList());
    }
}
