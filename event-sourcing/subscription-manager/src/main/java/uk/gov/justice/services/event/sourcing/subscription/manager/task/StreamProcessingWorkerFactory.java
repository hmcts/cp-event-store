package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.subscription.SourceComponentPair;

import javax.inject.Inject;

public class StreamProcessingWorkerFactory {

    @Inject
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Inject
    private WorkerActivityTracker workerActivityTracker;

    public StreamProcessingWorkerTask createWorkerTask(final SourceComponentPair sourceComponentPair) {
        return new StreamProcessingWorkerTask(
                streamProcessingWorkerBean,
                workerActivityTracker,
                sourceComponentPair);
    }
}
