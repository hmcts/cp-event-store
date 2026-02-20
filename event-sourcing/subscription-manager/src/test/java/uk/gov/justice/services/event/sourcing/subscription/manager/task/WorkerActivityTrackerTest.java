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
    public void shouldTrackRecentlyActiveState() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        assertThat(workerActivityTracker.isRecentlyActive(pair), is(false));

        workerActivityTracker.markRecentlyActive(pair);
        assertThat(workerActivityTracker.isRecentlyActive(pair), is(true));

        final boolean wasActive = workerActivityTracker.resetRecentlyActive(pair);
        assertThat(wasActive, is(true));
        assertThat(workerActivityTracker.isRecentlyActive(pair), is(false));
    }

    @Test
    public void shouldTrackLastProbeTime() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        assertThat(workerActivityTracker.getLastProbeTime(pair), is(0L));

        workerActivityTracker.setLastProbeTime(pair, 12345L);
        assertThat(workerActivityTracker.getLastProbeTime(pair), is(12345L));
    }

    @Test
    public void shouldTrackStateSeparatelyForDifferentPairs() {
        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component-1");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component-2");

        workerActivityTracker.incrementActiveCount(pair1);
        workerActivityTracker.markRecentlyActive(pair1);

        assertThat(workerActivityTracker.getActiveCount(pair1), is(1));
        assertThat(workerActivityTracker.getActiveCount(pair2), is(0));
        assertThat(workerActivityTracker.isRecentlyActive(pair1), is(true));
        assertThat(workerActivityTracker.isRecentlyActive(pair2), is(false));
    }

    @Test
    public void shouldReturnSameStateForSamePair() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        final WorkerActivityTracker.WorkerState state1 = workerActivityTracker.getOrCreateState(pair);
        final WorkerActivityTracker.WorkerState state2 = workerActivityTracker.getOrCreateState(pair);

        assertThat(state1, is(state2));
    }
}
