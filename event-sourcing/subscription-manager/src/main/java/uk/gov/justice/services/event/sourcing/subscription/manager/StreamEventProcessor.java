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

        final Optional<LockedStreamStatus> lockedStreamStatus;
        try {
            transactionHandler.begin(userTransaction);
            lockedStreamStatus = streamSelector.findStreamToProcess(source, component);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(format("Failed to find stream to process, source: '%s', component: '%s'", source, component), e);
        }

        if (lockedStreamStatus.isPresent()) {
            JsonEnvelope eventJsonEnvelope = null;
            final long currentPosition = lockedStreamStatus.get().position();
            final long latestKnownPosition = lockedStreamStatus.get().latestKnownPosition();
            final UUID streamId = lockedStreamStatus.get().streamId();
            String eventName = null;
            UUID eventId = null;

            try {
                eventJsonEnvelope = findNextEventInTheStreamAfterPosition(source, streamId, currentPosition);
                final Metadata metadata = eventJsonEnvelope.metadata();
                eventName = metadata.name();
                eventId = metadata.id();
                final Long eventPositionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));

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
                streamErrorStatusHandler.onStreamProcessingFailure(eventJsonEnvelope, e, component, currentPosition);
                throw new StreamProcessingException(format("Failed to process event. name: '%s', eventId: '%s', streamId: '%s'", eventName, eventId, streamId), e);
            }
        } else {
            try {
                transactionHandler.commit(userTransaction);
            } catch (final Exception e) {
                transactionHandler.rollback(userTransaction);
            }
            return false;
        }
    }

    private JsonEnvelope findNextEventInTheStreamAfterPosition(final String source, final UUID streamId, final long position) {
        final LinkedEventSource linkedEventSource = linkedEventSourceProvider.getLinkedEventSource(source);
        Optional<LinkedEvent> linkedEvent = linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, position);
        return linkedEvent.map(eventConverter::envelopeOf).orElseThrow(() -> new StreamProcessingException(
                format("Failed to find event to process, streamId: '%s', position: '%d'", streamId, position)));
    }
}