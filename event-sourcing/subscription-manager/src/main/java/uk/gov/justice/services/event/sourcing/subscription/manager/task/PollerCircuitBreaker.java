package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;
import uk.gov.justice.subscription.SourceComponentPair;

import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class PollerCircuitBreaker {

    private final ConcurrentHashMap<SourceComponentPair, CircuitState> circuitMap = new ConcurrentHashMap<>();

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    @Inject
    private Logger logger;

    @Inject
    private UtcClock clock;

    public boolean isOpen(final String source, final String component) {
        return getCircuitState(source, component).isOpen();
    }

    public boolean isCircuitTripped(final String source, final String component) {
        return getCircuitState(source, component).isTripped();
    }

    public boolean tryTransitionToProbe(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        return state.tryAcquireProbeSlot(currentMillis(), streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds(),
                source, component, logger);
    }

    public void recordSuccess(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        state.onSuccess(source, component, logger);
    }

    public void recordFailure(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        state.onFailure(source, component,
                streamProcessingConfig.getCircuitBreakerFailureThreshold(),
                currentMillis(),
                logger);
    }

    private long currentMillis() {
        return clock.now().toInstant().toEpochMilli();
    }

    private CircuitState getCircuitState(String source, String component) {
        return circuitMap.computeIfAbsent(
                new SourceComponentPair(source, component), k -> new CircuitState());
    }

}
