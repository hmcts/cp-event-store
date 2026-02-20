package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.util.Optional;
import java.util.UUID;

import javax.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NextEventSelectorTest {

    @Mock
    private NextEventReader nextEventReader;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private UserTransaction userTransaction;

    @InjectMocks
    private NextEventSelector nextEventSelector;

    @Test
    public void shouldReturnPulledEventWhenEventFound() {
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);

        when(nextEventReader.read(streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));

        final Optional<PulledEvent> result = nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus));

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().jsonEnvelope(), is(eventJsonEnvelope));
        assertThat(result.get().lockedStreamStatus(), is(lockedStreamStatus));
        verifyNoInteractions(transactionHandler);
    }

    @Test
    public void shouldReturnEmptyWhenNoLockedStreamStatus() {
        final String source = "some-source";
        final String component = "some-component";

        final Optional<PulledEvent> result = nextEventSelector.selectNextEvent(source, component, empty());

        assertThat(result.isPresent(), is(false));
        verifyNoInteractions(nextEventReader);
        verifyNoInteractions(transactionHandler);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenEventNotFound() {
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());

        when(nextEventReader.read(streamId, currentPosition)).thenReturn(empty());

        final StreamProcessingException exception = assertThrows(
                StreamProcessingException.class,
                () -> nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus)));

        assertThat(exception.getMessage(), is("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));
        verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        verify(transactionHandler).rollback(userTransaction);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenFindingEventFails() {
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final RuntimeException eventFindingException = new RuntimeException("Event finding failed");

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());

        when(nextEventReader.read(streamId, currentPosition)).thenThrow(eventFindingException);

        final StreamProcessingException exception = assertThrows(
                StreamProcessingException.class,
                () -> nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus)));

        assertThat(exception.getMessage(), is("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));
        verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        verify(transactionHandler).rollback(userTransaction);
    }
}
