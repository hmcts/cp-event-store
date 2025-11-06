package uk.gov.justice.services.eventstore.management.replay.process;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ReplayEventToComponentRunnerTest {

    private static final UUID COMMAND_ID = UUID.randomUUID();
    private static final UUID COMMAND_RUNTIME_ID = UUID.randomUUID();

    @Mock
    private EventSourceNameFinder eventSourceNameFinder;

    @Mock
    private ReplayEventToEventListenerProcessorBean replayEventToEventListenerProcessorBean;

    @InjectMocks
    private ReplayEventToComponentRunner replayEventToComponentRunner;

    @Test
    public void buildContextWithEventSourceNameAndPassItToProcessorBean() {
        final String listenerEventSourceName = "listenerEventSourceName";
        final String componentName = EVENT_LISTENER;
        when(eventSourceNameFinder.getEventSourceNameOf(componentName)).thenReturn(listenerEventSourceName);

        replayEventToComponentRunner.run(COMMAND_ID, COMMAND_RUNTIME_ID, componentName);

        verify(replayEventToEventListenerProcessorBean).perform(argThat(actualContext -> {
            assertThat(actualContext.getCommandId(), is(COMMAND_ID));
            assertThat(actualContext.getCommandRuntimeId(), is(COMMAND_RUNTIME_ID));
            assertThat(actualContext.getEventSourceName(), is(listenerEventSourceName));
            assertThat(actualContext.getComponentName(), is(EVENT_LISTENER));

            return true;
        }));
    }
}