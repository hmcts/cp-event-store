package uk.gov.justice.services.eventstore.management.commands;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

public class VerifyCatchupCommandTest {

    @Test
    public void shouldBeCatchupVerification() throws Exception {
        assertThat(new VerifyCatchupCommand().isCatchupVerification(), is(true));
    }

    @Test
    public void shouldBeDisabledByPullMechanism() throws Exception {
        assertThat(new VerifyCatchupCommand().isDisabledByPullMechanism(), is(true));
    }
}
