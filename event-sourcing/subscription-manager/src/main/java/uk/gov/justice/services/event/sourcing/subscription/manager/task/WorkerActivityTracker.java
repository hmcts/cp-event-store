package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.subscription.SourceComponentPair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WorkerActivityTracker {

    private final ConcurrentHashMap<SourceComponentPair, AtomicInteger> activeCountMap = new ConcurrentHashMap<>();

    public int getActiveCount(final SourceComponentPair pair) {
        return activeCountMap.computeIfAbsent(pair, k -> new AtomicInteger(0)).get();
    }

    public int incrementActiveCount(final SourceComponentPair pair) {
        return activeCountMap.computeIfAbsent(pair, k -> new AtomicInteger(0)).incrementAndGet();
    }

    public int decrementActiveCount(final SourceComponentPair pair) {
        return activeCountMap.compute(pair, (k, current) -> {
            final int currentValue = (current == null) ? 0 : current.get();
            return new AtomicInteger(Math.max(0, currentValue - 1));
        }).get();
    }
}
