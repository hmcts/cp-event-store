package uk.gov.justice.services.eventstore.management.commands;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IndexerCatchupCommandTest {

    @InjectMocks
    private IndexerCatchupCommand indexerCatchupCommand;

    @Test
    public void shouldBeIndexerCatchupCommand() throws Exception {
        assertThat(indexerCatchupCommand.isEventCatchup(), is(false));
    }

    @Test
    public void shouldBeDisabledByPullMechanism() throws Exception {
        assertThat(new VerifyCatchupCommand().isDisabledByPullMechanism(), is(true));
    }
}
