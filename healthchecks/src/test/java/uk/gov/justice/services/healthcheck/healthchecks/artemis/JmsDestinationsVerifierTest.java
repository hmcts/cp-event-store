package uk.gov.justice.services.healthcheck.healthchecks.artemis;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import uk.gov.justice.services.messaging.jms.DestinationProvider;
import uk.gov.justice.services.messaging.jms.exception.JmsEnvelopeSenderException;

import java.util.List;

import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class JmsDestinationsVerifierTest {

    @Mock
    private Logger logger;

    @Mock
    private DestinationNamesProvider destinationNamesProvider;

    @Mock
    private DestinationProvider destinationProvider;

    @InjectMocks
    private JmsDestinationsVerifier jmsDestinationsVerifier;

    @Captor
    ArgumentCaptor<String> destinationNameCaptor;

    @Test
    public void shouldVerifyThatAllDestinationsExists() throws Exception {
        var session = mock(Session.class);
        var destination = mock(Destination.class);
        var consumer = mock(MessageConsumer.class);
        given(destinationNamesProvider.getDestinationNames()).willReturn(List.of("queue-1", "queue-2"));
        given(destinationProvider.getDestination(any())).willReturn(destination);
        given(session.createConsumer(destination)).willReturn(consumer);

        jmsDestinationsVerifier.verify(session);

        verify(destinationProvider, times(2)).getDestination(destinationNameCaptor.capture());
        assertThat(destinationNameCaptor.getAllValues(), hasItems("queue-1", "queue-2"));
        verify(session, times(2)).createConsumer(destination);
        verify(consumer, times(2)).close();
    }

    @Test
    public void shouldThrowExceptionWhenDestinationNotExist() throws Exception {
        var session = mock(Session.class);
        given(destinationNamesProvider.getDestinationNames()).willReturn(List.of("queue-1"));
        doThrow(new JmsEnvelopeSenderException("Ex message", new RuntimeException())).when(destinationProvider).getDestination("queue-1");

        assertThrows(DestinationNotFoundException.class, () -> jmsDestinationsVerifier.verify(session));
    }

    @Test
    public void shouldContinueVerifyingNextDestinationWhenExceptionThrownByPreviousDestinationAndFinallyThrowException() throws Exception {
        var session = mock(Session.class);
        var destination = mock(Destination.class);
        var consumer = mock(MessageConsumer.class);
        var cause = new JmsEnvelopeSenderException("Ex message", new RuntimeException());
        given(destinationNamesProvider.getDestinationNames()).willReturn(List.of("queue-1", "queue-2", "queue-3"));
        doThrow(cause).when(destinationProvider).getDestination("queue-1");
        doThrow(cause).when(destinationProvider).getDestination("queue-3");
        given(destinationProvider.getDestination("queue-2")).willReturn(destination);
        given(session.createConsumer(destination)).willReturn(consumer);

        boolean exceptionThrown = false;
        try{
            jmsDestinationsVerifier.verify(session);
        } catch (Exception e) {
            exceptionThrown = true;
            assertTrue(e instanceof DestinationNotFoundException);
            var destinationNotFoundException = (DestinationNotFoundException)e;
            assertThat(destinationNotFoundException.getMessage(), CoreMatchers.is("Destination(s) queue-1, queue-3 not exist"));
            verify(logger, times(2)).error(anyString(), any(Throwable.class));
        }

        assertTrue(exceptionThrown);
    }
}