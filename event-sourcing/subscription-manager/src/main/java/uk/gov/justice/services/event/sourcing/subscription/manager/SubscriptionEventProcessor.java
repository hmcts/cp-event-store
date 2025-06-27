package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;

public class SubscriptionEventProcessor {

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    @Inject
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Inject
    public InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private NewEventBufferRepository newEventBufferRepository;

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Inject
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Transactional(value = NOT_SUPPORTED)
    public boolean processSingleEvent(
            final JsonEnvelope eventJsonEnvelope,
            final String component) {

        final Metadata metadata = eventJsonEnvelope.metadata();
        final String name = metadata.name();
        final UUID eventId = metadata.id();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(format("No streamId found in event: name '%s', eventId '%s'", name, eventId)));
        final String source = eventSourceNameCalculator.getSource(eventJsonEnvelope);
        final Long eventPositionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", name, eventId)));

        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);
        
        try {
            transactionHandler.begin(userTransaction);

            final StreamPositions streamPositions = newStreamStatusRepository.lockRowAndGetPositions(streamId, source, component, eventPositionInStream);
            final EventOrderingStatus eventOrderingStatus = eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions);

            final AtomicBoolean eventProcessed = new AtomicBoolean(false);
            if (eventOrderingStatus == EVENT_CORRECTLY_ORDERED) {
                final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer.produceLocalProcessor(component);
                final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(eventJsonEnvelope);

                interceptorChainProcessor.process(interceptorContext);

                newStreamStatusRepository.updateCurrentPosition(streamId, source, component, eventPositionInStream);
                newEventBufferRepository.remove(streamId, source, component, eventPositionInStream);
                streamErrorRepository.markStreamAsFixed(streamId, source, component);

                if (streamPositions.latestKnownStreamPosition() == eventPositionInStream) {
                    newStreamStatusRepository.setUpToDate(true, streamId, source, component);
                }

                eventProcessed.set(true);

                micrometerMetricsCounters.incrementEventsSucceededCount(source, component);
            }   else {
                micrometerMetricsCounters.incrementEventsIgnoredCount(source, component);
            }

            transactionHandler.commit(userTransaction);

            return eventProcessed.get();

        } catch (final Throwable e) {
            transactionHandler.rollback(userTransaction);
            streamErrorStatusHandler.onStreamProcessingFailure(eventJsonEnvelope, e, source, component);
            throw new StreamProcessingException(format("Failed to process event. name: '%s', eventId: '%s', streamId: '%s'", name, eventId, streamId), e);
        }
    }
}