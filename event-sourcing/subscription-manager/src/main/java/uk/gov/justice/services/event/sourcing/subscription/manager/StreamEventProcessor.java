package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;

public class StreamEventProcessor {

    @Inject
    public InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    @Inject
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Inject
    private StreamSelector streamSelector;

    @Inject
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Inject
    private EventConverter eventConverter;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Transactional(value = NOT_SUPPORTED)
    public boolean processSingleEvent(final String source, final String component) {
        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);

        transactionHandler.begin(userTransaction);

        Optional<PulledEvent> pulledEvent = pullEventToProcess(source, component);

        if (pulledEvent.isPresent()) {
            final JsonEnvelope eventJsonEnvelope = pulledEvent.get().jsonEnvelope();
            final LockedStreamStatus lockedStreamStatus = pulledEvent.get().lockedStreamStatus();
            final UUID streamId = lockedStreamStatus.streamId();
            final long latestKnownPosition = lockedStreamStatus.latestKnownPosition();
            final long streamCurrentPosition = lockedStreamStatus.position();
            final Metadata metadata = eventJsonEnvelope.metadata();

            try {
                final long eventPositionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));

                final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer.produceLocalProcessor(component);
                final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(eventJsonEnvelope);
                interceptorChainProcessor.process(interceptorContext);

                newStreamStatusRepository.updateCurrentPosition(streamId, source, component, eventPositionInStream);

                if (latestKnownPosition == eventPositionInStream) {
                    newStreamStatusRepository.setUpToDate(true, streamId, source, component);
                }

                micrometerMetricsCounters.incrementEventsSucceededCount(source, component);

                transactionHandler.commit(userTransaction);

                return true;

            } catch (final Exception e) {
                transactionHandler.rollback(userTransaction);
                micrometerMetricsCounters.incrementEventsFailedCount(source, component);
                streamErrorStatusHandler.onStreamProcessingFailure(eventJsonEnvelope, e, component, streamCurrentPosition);
                throw new StreamProcessingException(format("Failed to process event. name: '%s', eventId: '%s', streamId: '%s'", metadata.name(), metadata.id(), streamId), e);
            }
        } else {
            commitWithFallBackToRollback();
            return false;
        }
    }

    private void commitWithFallBackToRollback() {
        try {
            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
        }
    }

    private Optional<PulledEvent> pullEventToProcess(final String source, final String component) {
        final Optional<LockedStreamStatus> lockedStreamStatus;
        try {
            lockedStreamStatus = streamSelector.findStreamToProcess(source, component);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(format("Failed to find stream to process, source: '%s', component: '%s'", source, component), e);
        }

        if (lockedStreamStatus.isPresent()) {
            final JsonEnvelope eventJsonEnvelope =  findNextEventInTheStreamAfterPosition(source, component, lockedStreamStatus.get());
            return Optional.of(new PulledEvent(eventJsonEnvelope, lockedStreamStatus.get()));
        }

        return Optional.empty();
    }

    private JsonEnvelope findNextEventInTheStreamAfterPosition(final String source, final String component, final LockedStreamStatus lockedStreamStatus) {
        final Optional<JsonEnvelope> eventJsonEnvelope;
        final UUID streamId = lockedStreamStatus.streamId();
        final Long position = lockedStreamStatus.position();
        final Long latestKnownPosition = lockedStreamStatus.latestKnownPosition();

        try {
            final LinkedEventSource linkedEventSource = linkedEventSourceProvider.getLinkedEventSource(source);
            Optional<LinkedEvent> linkedEvent = linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, position);
            eventJsonEnvelope = linkedEvent.map(eventConverter::envelopeOf);
        } catch (Exception e) {
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(
                    format("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition));
        }

        //TODO revisit this later to understand the requirement on whether to mark the stream as failed if this ever happens, but with current db schema without an event stream can not be marked as error
        return eventJsonEnvelope.orElseThrow(() -> {
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(
                    format("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition));
        });
    }

    private record PulledEvent(JsonEnvelope jsonEnvelope, LockedStreamStatus lockedStreamStatus) {
    }
}