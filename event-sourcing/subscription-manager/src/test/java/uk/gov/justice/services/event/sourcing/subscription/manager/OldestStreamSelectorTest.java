package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OldestStreamSelectorTest {

    @Mock
    private NewStreamStatusRepository streamStatusRepository;

    @Mock
    private StreamRetryConfiguration streamRetryConfiguration;

    @InjectMocks
    private OldestStreamSelector oldestStreamSelector;

    @Test
    public void shouldFindStreamToProcess() {

        final String source = "some-source";
        final String component = "some-component";
        final UUID streamId = randomUUID();
        final long position = 5L;
        final long latestKnownPosition = 10L;
        final int maxRetries = 10;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, position, latestKnownPosition);

        when(streamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, component, maxRetries))
                .thenReturn(of(lockedStreamStatus));
        when(streamRetryConfiguration.getMaxRetries()).thenReturn(maxRetries);

        final Optional<LockedStreamStatus> streamToProcess = oldestStreamSelector.findStreamToProcess(source, component);

        assertThat(streamToProcess.isPresent(), is(true));
        assertThat(streamToProcess.get().streamId(), is(streamId));
        assertThat(streamToProcess.get().position(), is(position));
        assertThat(streamToProcess.get().latestKnownPosition(), is(latestKnownPosition));

        verify(streamStatusRepository).findOldestStreamToProcessByAcquiringLock(source, component, maxRetries);
    }

    @Test
    public void shouldReturnEmptyWhenNoStreamFound() {

        final String source = "some-source";
        final String component = "some-component";
        final int maxRetries = 10;

        when(streamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, component, maxRetries))
                .thenReturn(empty());
        when(streamRetryConfiguration.getMaxRetries()).thenReturn(maxRetries);

        final Optional<LockedStreamStatus> streamToProcess = oldestStreamSelector.findStreamToProcess(source, component);

        assertThat(streamToProcess, is(empty()));

        verify(streamStatusRepository).findOldestStreamToProcessByAcquiringLock(source, component, maxRetries);
    }
}
