package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;
import uk.gov.justice.subscription.SourceComponentPair;

import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class PollerCircuitBreaker {

    private final ConcurrentHashMap<SourceComponentPair, CircuitState> circuitMap = new ConcurrentHashMap<>();

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    @Inject
    private Logger logger;

    public boolean isOpen(final String source, final String component) {
        return getCircuitState(source, component).isOpen();
    }

    public boolean isCircuitTripped(final String source, final String component) {
        return getCircuitState(source, component).isTripped();
    }

    public boolean tryTransitionToProbe(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        return state.tryAcquireProbeSlot(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds());
    }

    public void recordSuccess(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        state.onSuccess(source, component, logger);
    }

    public void recordFailure(final String source, final String component) {
        final CircuitState state = getCircuitState(source, component);
        state.onFailure(source, component,
                streamProcessingConfig.getCircuitBreakerFailureThreshold(),
                logger);
    }

    private CircuitState getCircuitState(String source, String component) {
        return circuitMap.computeIfAbsent(
                new SourceComponentPair(source, component), k -> new CircuitState());
    }

}
