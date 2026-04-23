package uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands;

import static java.util.UUID.fromString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.services.core.aggregate.SnapshotAwareAggregateService;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;

import java.util.UUID;
import java.util.stream.Stream;

import jakarta.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class RegenerateAggregateSnapshotBeanTest {

    @Mock
    private SnapshotAwareAggregateService snapshotAwareAggregateService;

    @Mock
    private EventSource eventSource;

    @Mock
    private AggregateClassProvider aggregateClassProvider;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private AggregateSnapshotGenerationCommandConfiguration aggregateSnapshotGenerationCommandConfiguration;

    @Mock
    private Logger logger;

    @InjectMocks
    private RegenerateAggregateSnapshotBean regenerateAggregateSnapshotBean;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldGetAggregateByNameAndStreamIdInOrderToGenerateSnapshots() throws Exception {

        final int transactionTimoutSeconds = 23;
        final UUID streamId = fromString("6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8");
        final Aggregate aggregate = new SomeAggregate();

        final Class aggregateClass = aggregate.getClass();
        final String aggregateClassName = aggregateClass.getName();
        final EventStream eventStream = mock(EventStream.class);

        when(aggregateSnapshotGenerationCommandConfiguration.getTransactionTimoutSeconds()).thenReturn(transactionTimoutSeconds);

        when(aggregateClassProvider.toClass(aggregateClassName)).thenReturn(aggregateClass);
        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(snapshotAwareAggregateService.get(eventStream, aggregateClass)).thenReturn(aggregate);

        regenerateAggregateSnapshotBean.runAggregateSnapshotRegeneration(streamId, aggregateClassName);

        final ArgumentCaptor<Stream> streamArgumentCaptor = ArgumentCaptor.forClass(Stream.class);

        final InOrder inOrder = inOrder(userTransaction, logger, eventStream, eventSource);
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(logger).info("Hydrating aggregate 'uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.SomeAggregate' for streamId '6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8'");
        inOrder.verify(eventSource).getStreamById(streamId);
        inOrder.verify(eventStream).append(streamArgumentCaptor.capture());
        inOrder.verify(logger).info("'uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.SomeAggregate' hydrated with all events for streamId '6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8'");
        inOrder.verify(userTransaction).commit();

        assertThat(streamArgumentCaptor.getValue().toList().isEmpty(), is(true));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldThrowAggregateSnapshotGenerationFailedExceptionIfSnapshotGenerationFails() throws Exception {

        final EventStreamException eventStreamException = new EventStreamException("ooops");
        final int transactionTimoutSeconds = 23;
        final UUID streamId = fromString("6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8");
        final Aggregate aggregate = new SomeAggregate();

        final Class aggregateClass = aggregate.getClass();
        final String aggregateClassName = aggregateClass.getName();
        final EventStream eventStream = mock(EventStream.class);

        when(aggregateSnapshotGenerationCommandConfiguration.getTransactionTimoutSeconds()).thenReturn(transactionTimoutSeconds);

        when(aggregateClassProvider.toClass(aggregateClassName)).thenReturn(aggregateClass);
        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(snapshotAwareAggregateService.get(eventStream, aggregateClass)).thenReturn(aggregate);
        doThrow(eventStreamException).when(eventStream).append(any(Stream.class));

        final AggregateSnapshotGenerationFailedException aggregateSnapshotGenerationFailedException = assertThrows(
                AggregateSnapshotGenerationFailedException.class,
                () -> regenerateAggregateSnapshotBean.runAggregateSnapshotRegeneration(
                        streamId,
                        aggregateClassName));

        assertThat(aggregateSnapshotGenerationFailedException.getMessage(), is("Snapshot generation failed for 'uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.SomeAggregate': streamId '6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8'"));
        assertThat(aggregateSnapshotGenerationFailedException.getCause(), is(eventStreamException));

        final ArgumentCaptor<Stream> streamArgumentCaptor = ArgumentCaptor.forClass(Stream.class);

        final InOrder inOrder = inOrder(userTransaction, logger, eventStream, eventSource);
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(logger).info("Hydrating aggregate 'uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.SomeAggregate' for streamId '6c01bc31-9ee2-4a3c-bdd2-6f20a619bae8'");
        inOrder.verify(eventSource).getStreamById(streamId);
        inOrder.verify(eventStream).append(streamArgumentCaptor.capture());
        inOrder.verify(userTransaction).rollback();

        inOrder.verify(userTransaction, never()).commit();

        assertThat(streamArgumentCaptor.getValue().toList().isEmpty(), is(true));
    }
}