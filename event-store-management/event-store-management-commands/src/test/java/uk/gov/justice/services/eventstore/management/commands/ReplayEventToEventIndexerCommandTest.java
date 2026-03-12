package uk.gov.justice.services.eventstore.management.commands;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

public class ReplayEventToEventIndexerCommandTest {

    @Test
    public void shouldBeDisabledByPullMechanism() throws Exception {
        assertThat(new ReplayEventToEventIndexerCommand().isDisabledByPullMechanism(), is(true));
    }
}