package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.CLOSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.HALF_OPEN;
import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.OPEN;

class CircuitState {

    enum State {CLOSED, OPEN, HALF_OPEN} // HALF_OPEN signals that a probe is in progress

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private volatile long openedAtMillis = 0L;
    private volatile long halfOpenedAtMillis = Long.MAX_VALUE;

    @VisibleForTesting
    State getState() {
        return state.get();
    }

    public boolean isOpen() {
        return stateIs(OPEN);
    }

    public boolean isTripped() {
        return !stateIs(CLOSED);
    }

    public boolean tryAcquireProbeSlot(final long currentTimeMillis, final long cooldownMillis,
                                       final String source, final String component,
                                       final Logger logger) {
        if (stateIs(HALF_OPEN) && halfOpenedTimeExceededCoolDown(currentTimeMillis, cooldownMillis)) {
            if (state.compareAndSet(State.HALF_OPEN, OPEN)) {
                openedAtMillis = 0L;
                logger.warn("Circuit breaker HALF_OPEN probe timed out after {}ms for source: {}, component: {}, resetting for new probe",
                        (currentTimeMillis - halfOpenedAtMillis), source, component);
            }
        }

        if (stateIs(OPEN) && openedTimeExceededCoolDown(currentTimeMillis, cooldownMillis)) {
            if (state.compareAndSet(OPEN, State.HALF_OPEN)) {
                halfOpenedAtMillis = currentTimeMillis;
                return true;
            }
        }

        return false;
    }

    public void onSuccess(final String source, final String component, final Logger logger) {
        if (stateIs(HALF_OPEN)) {
            consecutiveFailures.set(0);
            state.set(State.CLOSED);
            logger.info("Circuit breaker CLOSED (probe succeeded) for source: {}, component: {}",
                    source, component);
        } else if (stateIs(CLOSED)) {
            consecutiveFailures.set(0);
        }
    }

    public void onFailure(final String source, final String component,
                          final int failureThreshold, final long currentTimeMillis,
                          final Logger logger) {
        if (stateIs(HALF_OPEN)) {
            openedAtMillis = currentTimeMillis;
            state.set(OPEN);
            logger.warn("Circuit breaker re-OPENED (probe failed again) for source: {}, component: {}",
                    source, component);
        } else if (stateIs(CLOSED)) {
            if (consecutiveFailuresExceeded(failureThreshold)) {
                openedAtMillis = currentTimeMillis;
                state.set(OPEN);
                logger.error("Circuit breaker OPENED after {} consecutive failures for source: {}, component: {}",
                        consecutiveFailures.get(), source, component);
            } else {
                logger.warn("Circuit breaker (while in CLOSED state) failure {} of {} for source: {}, component: {}",
                        consecutiveFailures.get(), failureThreshold, source, component);
            }
        }
    }

    private boolean consecutiveFailuresExceeded(int failureThreshold) {
        return consecutiveFailures.incrementAndGet() >= failureThreshold;
    }

    private boolean stateIs(State expectedState) {
        return state.get() == expectedState;
    }

    private boolean halfOpenedTimeExceededCoolDown(final long currentTimeMillis, final long cooldownMillis) {
        return (currentTimeMillis - halfOpenedAtMillis) > cooldownMillis;
    }

    private boolean openedTimeExceededCoolDown(final long currentTimeMillis, final long cooldownMillis) {
        return (currentTimeMillis - openedAtMillis) >= cooldownMillis;
    }
}
