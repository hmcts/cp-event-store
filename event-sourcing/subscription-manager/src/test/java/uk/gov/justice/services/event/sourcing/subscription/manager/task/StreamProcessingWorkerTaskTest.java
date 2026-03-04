package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import uk.gov.justice.subscription.SourceComponentPair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingWorkerTaskTest {

    @Mock
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Mock
    private WorkerActivityTracker workerActivityTracker;

    @Test
    public void shouldIncrementActiveCountBeforeProcessingAndDecrementAfter() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        final StreamProcessingWorkerTask task = new StreamProcessingWorkerTask(
                streamProcessingWorkerBean, workerActivityTracker, pair);

        task.run();

        final InOrder inOrder = inOrder(workerActivityTracker, streamProcessingWorkerBean);
        inOrder.verify(workerActivityTracker).incrementActiveCount(pair);
        inOrder.verify(streamProcessingWorkerBean).processUntilIdle("source", "component");
        inOrder.verify(workerActivityTracker).decrementActiveCount(pair);
    }

    @Test
    public void shouldDecrementActiveCountEvenWhenExceptionThrown() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        doThrow(new RuntimeException("Unexpected error"))
                .when(streamProcessingWorkerBean).processUntilIdle("source", "component");

        final StreamProcessingWorkerTask task = new StreamProcessingWorkerTask(
                streamProcessingWorkerBean, workerActivityTracker, pair);

        try {
            task.run();
        } catch (final RuntimeException ignored) {
        }

        verify(workerActivityTracker).decrementActiveCount(pair);
    }
}
