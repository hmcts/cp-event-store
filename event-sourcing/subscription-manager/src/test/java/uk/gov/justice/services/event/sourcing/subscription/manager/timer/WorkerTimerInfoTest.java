package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import org.junit.jupiter.api.Test;
import uk.gov.justice.subscription.SourceComponentPair;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class WorkerTimerInfoTest {

    @Test
    public void shouldCreateWorkerTimerInfoWithSourceComponentPairAndWorkerNumber() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");
        final int workerNumber = 2;

        final WorkerTimerInfo workerTimerInfo = new WorkerTimerInfo(pair, workerNumber);

        assertThat(workerTimerInfo.sourceComponentPair(), is(pair));
        assertThat(workerTimerInfo.workerNumber(), is(workerNumber));
    }

    @Test
    public void shouldBeEqualWhenSourceComponentPairAndWorkerNumberMatch() {
        final SourceComponentPair pair1 = new SourceComponentPair("source", "component");
        final SourceComponentPair pair2 = new SourceComponentPair("source", "component");

        final WorkerTimerInfo info1 = new WorkerTimerInfo(pair1, 0);
        final WorkerTimerInfo info2 = new WorkerTimerInfo(pair2, 0);

        assertThat(info1, is(info2));
        assertThat(info1.hashCode(), is(info2.hashCode()));
    }

    @Test
    public void shouldNotBeEqualWhenWorkerNumbersDiffer() {
        final SourceComponentPair pair = new SourceComponentPair("source", "component");

        final WorkerTimerInfo info1 = new WorkerTimerInfo(pair, 0);
        final WorkerTimerInfo info2 = new WorkerTimerInfo(pair, 1);

        assertThat(info1, is(not(info2)));
    }

    @Test
    public void shouldNotBeEqualWhenSourceComponentPairsDiffer() {
        final SourceComponentPair pair1 = new SourceComponentPair("source-1", "component");
        final SourceComponentPair pair2 = new SourceComponentPair("source-2", "component");

        final WorkerTimerInfo info1 = new WorkerTimerInfo(pair1, 0);
        final WorkerTimerInfo info2 = new WorkerTimerInfo(pair2, 0);

        assertThat(info1, is(not(info2)));
    }
}
