package uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands;

import static java.lang.String.format;
import static java.util.stream.Stream.empty;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.services.core.aggregate.SnapshotAwareAggregateService;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;
import java.util.stream.Stream;

import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionManagement;
import jakarta.ejb.TransactionManagementType;
import jakarta.inject.Inject;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;

import org.slf4j.Logger;

@Stateless
@TransactionManagement(TransactionManagementType.BEAN)
public class RegenerateAggregateSnapshotBean {

    @Inject
    private SnapshotAwareAggregateService snapshotAwareAggregateService;

    @Inject
    private EventSource eventSource;

    @Inject
    private AggregateClassProvider aggregateClassProvider;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private AggregateSnapshotGenerationCommandConfiguration aggregateSnapshotGenerationCommandConfiguration;

    @Inject
    private Logger logger;

    public void runAggregateSnapshotRegeneration(final UUID streamId, final String aggregateClassName) {

        try {
            final int transactionTimoutSeconds = aggregateSnapshotGenerationCommandConfiguration.getTransactionTimoutSeconds();
            userTransaction.setTransactionTimeout(transactionTimoutSeconds);
            userTransaction.begin();

            logger.info(format("Hydrating aggregate '%s' for streamId '%s'", aggregateClassName, streamId));

            final Class<? extends Aggregate> aggregateClass = aggregateClassProvider.toClass(aggregateClassName);
            final EventStream eventStream = eventSource.getStreamById(streamId);
            final Aggregate aggregate = snapshotAwareAggregateService.get(eventStream, aggregateClass);
            // to save, let's append
            final Stream<JsonEnvelope> empty = empty();
            eventStream.append(empty);

            logger.info(format("'%s' hydrated with all events for streamId '%s'", aggregate.getClass().getName(), streamId));

            userTransaction.commit();

        } catch (final Exception e) {
            try {
                userTransaction.rollback();
            } catch (final SystemException ignored) {}

            throw new AggregateSnapshotGenerationFailedException(format("Snapshot generation failed for '%s': streamId '%s'", aggregateClassName, streamId), e);
        }
    }

}
