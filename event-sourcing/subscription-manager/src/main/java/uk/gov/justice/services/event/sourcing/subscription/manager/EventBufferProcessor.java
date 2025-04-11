package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.stream.Stream;

import javax.transaction.Transactional;

public class EventBufferProcessor {

    private final InterceptorChainProcessor interceptorChainProcessor;
    private final EventBufferService eventBufferService;
    private final InterceptorContextProvider interceptorContextProvider;
    private final String component;
    private final StreamErrorRepository streamErrorRepository;

    public EventBufferProcessor(
            final InterceptorChainProcessor interceptorChainProcessor,
            final EventBufferService eventBufferService,
            final StreamErrorRepository streamErrorRepository,
            final InterceptorContextProvider interceptorContextProvider,
            final String component) {
        this.interceptorChainProcessor = interceptorChainProcessor;
        this.eventBufferService = eventBufferService;
        this.streamErrorRepository = streamErrorRepository;
        this.interceptorContextProvider = interceptorContextProvider;
        this.component = component;
    }

    public int processWithEventBuffer(final JsonEnvelope incomingJsonEnvelope) {

        final TallyCounter tallyCounter = new TallyCounter();
        try (final Stream<JsonEnvelope> jsonEnvelopeStream = eventBufferService.currentOrderedEventsWith(incomingJsonEnvelope, component)) {
            jsonEnvelopeStream.forEach(jsonEnvelope -> {
                final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(jsonEnvelope);
                interceptorChainProcessor.process(interceptorContext);
                tallyCounter.incrementByOne();
            });
        }

        return tallyCounter.getTallyCount();
    }
}

