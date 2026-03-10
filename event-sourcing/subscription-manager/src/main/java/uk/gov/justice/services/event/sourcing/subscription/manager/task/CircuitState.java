package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

class CircuitState {

    enum State { CLOSED, OPEN, HALF_OPEN } // HALF_OPEN state itself signals that a probe is in progress

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private volatile long openedAtMillis = 0L;

    State getState() {
        return state.get();
    }

    boolean allowRequest(final long cooldownMillis) {
        switch (state.get()) {
            case CLOSED:
                return true;
            case OPEN:
                final long elapsed = System.currentTimeMillis() - openedAtMillis;
                if (elapsed < cooldownMillis) {
                    return false;
                }
                return state.compareAndSet(State.OPEN, State.HALF_OPEN);
            default:
                return false;
        }
    }

    void onSuccess(final String source, final String component, final Logger logger) {
        if (state.get() == State.HALF_OPEN) {
            consecutiveFailures.set(0);
            state.set(State.CLOSED);
            logger.info("Circuit breaker CLOSED (probe succeeded) for source: {}, component: {}",
                    source, component);
        } else if (state.get() == State.CLOSED) {
            consecutiveFailures.set(0);
        }
    }

    void onFailure(final String source, final String component,
                   final int failureThreshold, final Logger logger) {
        if (state.get() == State.HALF_OPEN) {
            openedAtMillis = System.currentTimeMillis();
            state.set(State.OPEN);
            logger.warn("Circuit breaker re-OPENED (probe failed again) for source: {}, component: {}",
                    source, component);
        } else if (state.get() == State.CLOSED) {
            final int failures = consecutiveFailures.incrementAndGet();
            if (failures >= failureThreshold) {
                openedAtMillis = System.currentTimeMillis();
                state.set(State.OPEN);
                logger.error("Circuit breaker OPENED after {} consecutive failures for source: {}, component: {}",
                        failureThreshold, source, component);
            } else {
                logger.warn("Circuit breaker (while in CLOSED state) failure {} of {} for source: {}, component: {}",
                        failures, failureThreshold, source, component);
            }
        }
    }
}
