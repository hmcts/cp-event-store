package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.subscription.SourceComponentPair;

public class StreamProcessingWorkerTask implements Runnable {

    private final StreamProcessingWorkerBean streamProcessingWorkerBean;
    private final WorkerActivityTracker workerActivityTracker;
    private final SourceComponentPair sourceComponentPair;

    public StreamProcessingWorkerTask(
            final StreamProcessingWorkerBean streamProcessingWorkerBean,
            final WorkerActivityTracker workerActivityTracker,
            final SourceComponentPair sourceComponentPair) {
        this.streamProcessingWorkerBean = streamProcessingWorkerBean;
        this.workerActivityTracker = workerActivityTracker;
        this.sourceComponentPair = sourceComponentPair;
    }

    @Override
    public void run() {
        workerActivityTracker.incrementActiveCount(sourceComponentPair);
        try {
            final boolean foundEvents = streamProcessingWorkerBean.processUntilIdle(
                    sourceComponentPair.source(),
                    sourceComponentPair.component());

            if (foundEvents) {
                workerActivityTracker.markRecentlyActive(sourceComponentPair);
            }
        } finally {
            workerActivityTracker.decrementActiveCount(sourceComponentPair);
        }
    }
}
