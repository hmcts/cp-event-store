package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.subscription.SourceComponentPair;

import org.junit.jupiter.api.Test;

public class WorkerActivityTrackerTest {

    private final WorkerActivityTracker workerActivityTracker = new WorkerActivityTracker();

    @Test
    public void shouldReturnZeroActiveCountForNewPair() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        assertThat(workerActivityTracker.getActiveCount(pair), is(0));
    }

    @Test
    public void shouldIncrementAndDecrementActiveCount() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        assertThat(workerActivityTracker.incrementActiveCount(pair), is(1));
        assertThat(workerActivityTracker.incrementActiveCount(pair), is(2));
        assertThat(workerActivityTracker.getActiveCount(pair), is(2));

        assertThat(workerActivityTracker.decrementActiveCount(pair), is(1));
        assertThat(workerActivityTracker.getActiveCount(pair), is(1));
    }

    @Test
    public void shouldTrackStateSeparatelyForDifferentPairs() {
        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component-1");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component-2");

        workerActivityTracker.incrementActiveCount(pair1);

        assertThat(workerActivityTracker.getActiveCount(pair1), is(1));
        assertThat(workerActivityTracker.getActiveCount(pair2), is(0));
    }
}
