package uk.gov.justice.services.eventstore.management.verification.process;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventstore.management.commands.VerifyCatchupCommand;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.ProcessedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.PublishedEventLinkVerifier;
import uk.gov.justice.services.eventstore.management.verification.process.verifiers.StreamBufferEmptyVerifier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class VerifierProviderTest {

    @Mock
    private AllEventsInStreamsVerifier allEventsInStreamsVerifier;

    @Mock
    private ProcessedEventLinkVerifier processedEventLinkVerifier;

    @Mock
    private PublishedEventLinkVerifier publishedEventLinkVerifier;

    @Mock
    private StreamBufferEmptyVerifier streamBufferEmptyVerifier;

    @InjectMocks
    private VerifierProvider verifierProvider;

    @Test
    public void shouldGetTheListOfAllCatchupVerifiersInTheCorrectOrder() throws Exception {

        final List<Verifier> verifiers = verifierProvider.getVerifiers(new VerifyCatchupCommand());

        assertThat(verifiers.size(), is(4));

        assertThat(verifiers.get(0), is(streamBufferEmptyVerifier));
        assertThat(verifiers.get(1), is(publishedEventLinkVerifier));
        assertThat(verifiers.get(2), is(processedEventLinkVerifier));
        assertThat(verifiers.get(3), is(allEventsInStreamsVerifier));
    }
}
