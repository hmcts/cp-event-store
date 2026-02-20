package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.subscription.SourceComponentPair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WorkerActivityTracker {

    private final ConcurrentHashMap<SourceComponentPair, WorkerState> stateMap = new ConcurrentHashMap<>();

    public WorkerState getOrCreateState(final SourceComponentPair pair) {
        return stateMap.computeIfAbsent(pair, k -> new WorkerState());
    }

    public int getActiveCount(final SourceComponentPair pair) {
        return getOrCreateState(pair).activeCount.get();
    }

    public boolean isRecentlyActive(final SourceComponentPair pair) {
        return getOrCreateState(pair).recentlyActive.get();
    }

    public void markRecentlyActive(final SourceComponentPair pair) {
        getOrCreateState(pair).recentlyActive.set(true);
    }

    public boolean resetRecentlyActive(final SourceComponentPair pair) {
        return getOrCreateState(pair).recentlyActive.getAndSet(false);
    }

    public long getLastProbeTime(final SourceComponentPair pair) {
        return getOrCreateState(pair).lastProbeTime.get();
    }

    public void setLastProbeTime(final SourceComponentPair pair, final long time) {
        getOrCreateState(pair).lastProbeTime.set(time);
    }

    public int incrementActiveCount(final SourceComponentPair pair) {
        return getOrCreateState(pair).activeCount.incrementAndGet();
    }

    public int decrementActiveCount(final SourceComponentPair pair) {
        return getOrCreateState(pair).activeCount.decrementAndGet();
    }

    public static class WorkerState {
        final AtomicInteger activeCount = new AtomicInteger(0);
        final AtomicBoolean recentlyActive = new AtomicBoolean(false);
        final AtomicLong lastProbeTime = new AtomicLong(0);

        public AtomicInteger getActiveCount() {
            return activeCount;
        }

        public AtomicBoolean getRecentlyActive() {
            return recentlyActive;
        }

        public AtomicLong getLastProbeTime() {
            return lastProbeTime;
        }
    }
}
