package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.subscription.SourceComponentPair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingWorkerFactoryTest {

    @Mock
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Mock
    private WorkerActivityTracker workerActivityTracker;

    @InjectMocks
    private StreamProcessingWorkerFactory streamProcessingWorkerFactory;

    @Test
    public void shouldCreateWorkerTaskForSourceComponentPair() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        final StreamProcessingWorkerTask task = streamProcessingWorkerFactory.createWorkerTask(pair);

        assertThat(task, is(notNullValue()));
    }
}
