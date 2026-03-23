package uk.gov.justice.services.eventsourcing.source.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.BeforeEach;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.enterprise.event.Event;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventAppendTriggerServiceTest {

    @Mock
    private TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    @Mock
    private Event<EventAppendedEvent> eventAppendedEventFirer;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventAppendTriggerService eventAppendTriggerService;

    @BeforeEach
    public void postConstruct() {
        setField(eventAppendTriggerService, "eventLinkerNotified", "true");
        eventAppendTriggerService.postConstruct();
    }

    @Test
    public void shouldRegisterTransactionListener() {
        eventAppendTriggerService.registerTransactionListener();

        verify(transactionSynchronizationRegistry).registerInterposedSynchronization(any(Synchronization.class));
    }

    @Test
    public void shouldNotRegisterTransactionListenerIfAlreadyRegistered() {
        eventAppendTriggerService.registerTransactionListener();
        eventAppendTriggerService.registerTransactionListener();

        verify(transactionSynchronizationRegistry).registerInterposedSynchronization(any(Synchronization.class));
    }

    @Test
    public void shouldNotRegisterTransactionListenerIfFlagFalse() {

        setField(eventAppendTriggerService, "shouldWorkerNotified", false);

        eventAppendTriggerService.registerTransactionListener();

        verify(transactionSynchronizationRegistry,never()).registerInterposedSynchronization(any(Synchronization.class));
    }

    @Test
    public void shouldLogExceptionIfRegistrationFails() {
        doThrow(new RuntimeException("Error")).when(transactionSynchronizationRegistry).registerInterposedSynchronization(any(Synchronization.class));

        eventAppendTriggerService.registerTransactionListener();

        verify(logger).warn(any(String.class), any(Exception.class));
    }

    @Test
    public void shouldFireEventOnTransactionCommit() {
        ArgumentCaptor<Synchronization> synchronizationCaptor = ArgumentCaptor.forClass(Synchronization.class);

        eventAppendTriggerService.registerTransactionListener();

        verify(transactionSynchronizationRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());

        Synchronization synchronization = synchronizationCaptor.getValue();
        synchronization.afterCompletion(Status.STATUS_COMMITTED);

        verify(eventAppendedEventFirer).fire(any(EventAppendedEvent.class));
    }

    @Test
    public void shouldNotFireEventOnTransactionRollback() {
        ArgumentCaptor<Synchronization> synchronizationCaptor = ArgumentCaptor.forClass(Synchronization.class);

        eventAppendTriggerService.registerTransactionListener();

        verify(transactionSynchronizationRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());

        Synchronization synchronization = synchronizationCaptor.getValue();
        synchronization.afterCompletion(Status.STATUS_ROLLEDBACK);

        verify(eventAppendedEventFirer, never()).fire(any(EventAppendedEvent.class));
    }

    @Test
    public void shouldResetTrackerAfterTransactionCompletion() {
        ArgumentCaptor<Synchronization> synchronizationCaptor = ArgumentCaptor.forClass(Synchronization.class);

        eventAppendTriggerService.registerTransactionListener();
        verify(transactionSynchronizationRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());

        Synchronization synchronization = synchronizationCaptor.getValue();
        synchronization.afterCompletion(Status.STATUS_COMMITTED);

        // Should be able to register again
        eventAppendTriggerService.registerTransactionListener();
        verify(transactionSynchronizationRegistry, org.mockito.Mockito.times(2)).registerInterposedSynchronization(any(Synchronization.class));
    }
}
