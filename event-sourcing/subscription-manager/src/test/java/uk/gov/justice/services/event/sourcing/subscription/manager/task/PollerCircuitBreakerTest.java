package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class PollerCircuitBreakerTest {

    private static final String SOURCE = "test-source";
    private static final String COMPONENT = "test-component";

    @Mock
    private StreamProcessingConfig streamProcessingConfig;

    @Mock
    private Logger logger;

    @InjectMocks
    private PollerCircuitBreaker pollerCircuitBreaker;

    @Test
    public void shouldStartWithClosedState() {
        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(false));
    }

    @Test
    public void shouldOpenCircuitUsingConfiguredFailureThreshold() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(2);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);
        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(false));

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);
        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(true));
    }

    @Test
    public void shouldReturnTrueFromIsOpenWhenCircuitIsOpen() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);

        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(true));
    }

    @Test
    public void shouldReturnFalseFromIsOpenWhenCircuitIsHalfOpen() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);
        when(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds()).thenReturn(0L);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);
        pollerCircuitBreaker.tryTransitionToProbe(SOURCE, COMPONENT);

        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(false));
        assertThat(pollerCircuitBreaker.isCircuitTripped(SOURCE, COMPONENT), is(true));
    }

    @Test
    public void shouldCloseCircuitAfterProbeSucceedsUsingConfiguredCooldown() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);
        when(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds()).thenReturn(0L);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);

        pollerCircuitBreaker.tryTransitionToProbe(SOURCE, COMPONENT); // allocate probe slot
        pollerCircuitBreaker.recordSuccess(SOURCE, COMPONENT);

        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(false));
        assertThat(pollerCircuitBreaker.isCircuitTripped(SOURCE, COMPONENT), is(false));
    }

    @Test
    public void shouldReturnTrueFromIsCircuitTrippedWhenOpen() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);

        assertThat(pollerCircuitBreaker.isCircuitTripped(SOURCE, COMPONENT), is(true));
    }

    @Test
    public void shouldReturnFalseFromIsCircuitTrippedWhenClosed() {
        assertThat(pollerCircuitBreaker.isCircuitTripped(SOURCE, COMPONENT), is(false));
    }

    @Test
    public void shouldReturnFalseFromTryTransitionToProbeWhenCooldownNotElapsed() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);
        when(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds()).thenReturn(30_000L);

        pollerCircuitBreaker.recordFailure(SOURCE, COMPONENT);

        assertThat(pollerCircuitBreaker.tryTransitionToProbe(SOURCE, COMPONENT), is(false));
        assertThat(pollerCircuitBreaker.isOpen(SOURCE, COMPONENT), is(true));
    }

    @Test
    public void shouldReturnFalseFromTryTransitionToProbeWhenCircuitIsClosed() {
        when(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds()).thenReturn(0L);

        assertThat(pollerCircuitBreaker.tryTransitionToProbe(SOURCE, COMPONENT), is(false));
    }

    @Test
    public void shouldMaintainIndependentCircuitStatePerSourceComponentPair() {
        when(streamProcessingConfig.getCircuitBreakerFailureThreshold()).thenReturn(1);

        pollerCircuitBreaker.recordFailure("source-A", "component-A");

        assertThat(pollerCircuitBreaker.isOpen("source-A", "component-A"), is(true));
        assertThat(pollerCircuitBreaker.isOpen("source-B", "component-B"), is(false));
    }
}
