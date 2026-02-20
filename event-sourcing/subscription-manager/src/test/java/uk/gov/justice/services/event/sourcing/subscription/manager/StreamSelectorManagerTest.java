package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;

import java.util.Optional;

import javax.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamSelectorManagerTest {

    @Mock
    private StreamSelector streamSelector;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private UserTransaction userTransaction;

    @InjectMocks
    private StreamSelectorManager streamSelectorManager;

    @Test
    public void shouldReturnLockedStreamStatusWhenStreamFound() {
        final String source = "some-source";
        final String component = "some-component";
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(randomUUID(), 5L, 10L, empty());

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));

        final Optional<LockedStreamStatus> result = streamSelectorManager.selectStreamToProcess(source, component);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), is(lockedStreamStatus));
    }

    @Test
    public void shouldReturnEmptyWhenNoStreamFound() {
        final String source = "some-source";
        final String component = "some-component";

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(empty());

        final Optional<LockedStreamStatus> result = streamSelectorManager.selectStreamToProcess(source, component);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    public void shouldRollbackAndThrowStreamProcessingExceptionWhenStreamSelectionFails() {
        final String source = "some-source";
        final String component = "some-component";
        final RuntimeException selectionException = new RuntimeException("Selection failed");

        when(streamSelector.findStreamToProcess(source, component)).thenThrow(selectionException);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamSelectorManager.selectStreamToProcess(source, component));

        assertThat(streamProcessingException.getMessage(), is("Failed to find stream to process, source: 'some-source', component: 'some-component'"));
        assertThat(streamProcessingException.getCause(), is(selectionException));
        verify(transactionHandler).rollback(userTransaction);
    }
}
