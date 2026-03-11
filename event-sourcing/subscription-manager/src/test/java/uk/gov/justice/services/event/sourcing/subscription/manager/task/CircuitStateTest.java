package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.CLOSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.HALF_OPEN;
import static uk.gov.justice.services.event.sourcing.subscription.manager.task.CircuitState.State.OPEN;

@ExtendWith(MockitoExtension.class)
public class CircuitStateTest {

    private static final String SOURCE = "test-source";
    private static final String COMPONENT = "test-component";
    private static final int FAILURE_THRESHOLD_3 = 3;
    private static final int FAILURE_THRESHOLD_1 = 1;
    private static final long COOLDOWN_MILLIS = 30_000L;
    private static final long EXPIRED_COOLDOWN_MILLIS = 0L;

    @Mock
    private Logger logger;

    @Nested
    class WhenClosed {

        @Test
        public void shouldAllowRequest() {
            final CircuitState circuitState = new CircuitState();

            assertThat(circuitState.getState(), is(CLOSED));
            assertThat(circuitState.isOpen(), is(false));
            assertThat(circuitState.isTripped(), is(false));
        }

        @Test
        public void shouldRemainClosedWhenFailuresBelowThreshold() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);

            assertThat(circuitState.getState(), is(CLOSED));
            assertThat(circuitState.isOpen(), is(false));
            assertThat(circuitState.isTripped(), is(false));
        }

        @Test
        public void shouldResetFailureCountOnSuccess() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
            circuitState.onSuccess(SOURCE, COMPONENT, logger);

            assertThat(circuitState.getState(), is(CLOSED));

            // failure count reset: need threshold failures again to open
            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);

            assertThat(circuitState.getState(), is(CLOSED));
            assertThat(circuitState.isOpen(), is(false));
            assertThat(circuitState.isTripped(), is(false));
        }

        @Nested
        class WhenClosedTransitionsToOpen {

            @Test
            public void shouldOpenAndBlockRequestsAfterConsecutiveFailuresReachThreshold() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);

                assertThat(circuitState.getState(), is(OPEN));
                assertThat(circuitState.tryAcquireProbeSlot(0L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
                verify(logger).error("Circuit breaker OPENED after {} consecutive failures for source: {}, component: {}",
                        FAILURE_THRESHOLD_3, SOURCE, COMPONENT);
            }
        }
    }

    @Nested
    class WhenOpen {

        @Test
        public void shouldBeOpenWhenInOpenState() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

            assertThat(circuitState.isOpen(), is(true));
        }

        @Test
        public void shouldBeTrippedWhenInOpenState() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);

            assertThat(circuitState.isOpen(), is(true));
            assertThat(circuitState.isTripped(), is(true));
        }

        @Test
        public void shouldBlockAllRequestsWhileCooldownNotElapsed() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);

            assertThat(circuitState.getState(), is(OPEN));
            assertThat(circuitState.tryAcquireProbeSlot(0L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
        }

        @Test
        public void shouldRemainOpenWhenFailureOccursWhileCooldownNotElapsed() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
            assertThat(circuitState.getState(), is(OPEN));

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
            assertThat(circuitState.getState(), is(OPEN));
            assertThat(circuitState.tryAcquireProbeSlot(0L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
        }

        @Test
        public void shouldRemainOpenWhenSuccessOccursForInFlightRequestAfterCircuitOpened() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
            assertThat(circuitState.getState(), is(OPEN));

            // race: a request that started while CLOSED completed successfully after circuit opened
            circuitState.onSuccess(SOURCE, COMPONENT, logger);
            assertThat(circuitState.getState(), is(OPEN));
            assertThat(circuitState.tryAcquireProbeSlot(0L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
        }

        @Nested
        class WhenOpenTransitionsToHalfOpen {

            @Test
            public void shouldAllowSingleProbeRequestAfterCooldownElapsed() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);

                assertThat(circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(true));
                assertThat(circuitState.getState(), is(HALF_OPEN));
                assertThat(circuitState.isTripped(), is(true));
            }

            @Test
            public void shouldNotBeOpenOnceTransitionedToHalfOpen() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger);

                assertThat(circuitState.getState(), is(HALF_OPEN));
                assertThat(circuitState.isTripped(), is(true));
            }

            @Test
            public void shouldBlockSubsequentRequestsOnceProbeSlotIsAllocated() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);

                assertThat(circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(true)); // probe wins
                assertThat(circuitState.getState(), is(HALF_OPEN));
                assertThat(circuitState.isTripped(), is(true));

                assertThat(circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false)); // blocked
                assertThat(circuitState.getState(), is(HALF_OPEN));
                assertThat(circuitState.isTripped(), is(true));
            }
        }
    }

    @Nested
    class WhenHalfOpen {

        @Nested
        class ProbeSucceeds {

            @Test
            public void shouldCloseCircuitAndAllowRequests() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // allocate probe slot
                assertThat(circuitState.getState(), is(HALF_OPEN));

                circuitState.onSuccess(SOURCE, COMPONENT, logger);

                assertThat(circuitState.getState(), is(CLOSED));
                assertThat(circuitState.isOpen(), is(false));
                verify(logger).info("Circuit breaker CLOSED (probe succeeded) for source: {}, component: {}",
                        SOURCE, COMPONENT);
            }

            @Test
            public void shouldResetFailureCountSoThresholdMustBeReachedAgainToReopen() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // allocate probe slot
                circuitState.onSuccess(SOURCE, COMPONENT, logger);

                assertThat(circuitState.getState(), is(CLOSED));

                // failure count reset: need threshold failures again to re-open
                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);
                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_3, 0L, logger);

                assertThat(circuitState.getState(), is(CLOSED));
                assertThat(circuitState.isOpen(), is(false));
            }
        }

        @Nested
        class ProbeWorkerDied {

            @Test
            public void shouldAllowNewProbeWhenHalfOpenProbeTimesOut() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // probe slot acquired at T=0
                assertThat(circuitState.getState(), is(HALF_OPEN));

                // simulate coordinator tick after cooldown has elapsed with no worker completing
                assertThat(circuitState.tryAcquireProbeSlot(COOLDOWN_MILLIS + 1, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(true));
                assertThat(circuitState.getState(), is(HALF_OPEN));
            }

            @Test
            public void shouldNotRecoverHalfOpenProbeUntilCooldownElapsed() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, FAILURE_THRESHOLD_1, 0L, logger);
                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // probe slot acquired at T=0
                assertThat(circuitState.getState(), is(HALF_OPEN));

                // T=1ms — probe is still within its timeout window
                assertThat(circuitState.tryAcquireProbeSlot(1L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
                assertThat(circuitState.getState(), is(HALF_OPEN));
            }
        }

        @Nested
        class ProbeFails {

            @Test
            public void shouldReopenCircuitAndBlockRequestsWhileCooldownNotElapsed() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // allocate probe slot
                assertThat(circuitState.getState(), is(HALF_OPEN));

                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

                assertThat(circuitState.getState(), is(OPEN));
                assertThat(circuitState.tryAcquireProbeSlot(0L, COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(false));
                verify(logger).warn("Circuit breaker re-OPENED (probe failed again) for source: {}, component: {}",
                        SOURCE, COMPONENT);
            }

            @Test
            public void shouldAllowNextProbeAfterCooldownElapses() {
                final CircuitState circuitState = new CircuitState();

                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);

                circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger); // first probe slot
                circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger); // probe fails, re-opens

                assertThat(circuitState.getState(), is(OPEN));

                assertThat(circuitState.tryAcquireProbeSlot(0L, EXPIRED_COOLDOWN_MILLIS, SOURCE, COMPONENT, logger), is(true)); // second probe slot
                assertThat(circuitState.getState(), is(HALF_OPEN));
            }
        }
    }

    @Nested
    class NotFeasibleTransitions {

        @Test
        public void openToClosedIsNotFeasible() {
            final CircuitState circuitState = new CircuitState();

            circuitState.onFailure(SOURCE, COMPONENT, 1, 0L, logger);
            assertThat(circuitState.getState(), is(OPEN));

            //onSuccess gets called only when state is CLOSED
            //race: a request that started while CLOSED completed successfully after circuit opened
            circuitState.onSuccess(SOURCE, COMPONENT, logger);
            assertThat(circuitState.getState(), is(OPEN));
        }
    }
}
