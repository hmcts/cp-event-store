package uk.gov.justice.services.subscription;

import static java.lang.Long.MAX_VALUE;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessedEventStreamSpliterator extends AbstractSpliterator<ProcessedEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessedEventStreamSpliterator.class);

    private final String source;
    private final String component;
    private final Long batchSize;
    private final Long runFromEventNumber;
    private final ProcessedEventTrackingRepository processedEventTrackingRepository;

    private Iterator<ProcessedEvent> processedEventsIterator;
    private Long currentEventNumber = -1L;

    public ProcessedEventStreamSpliterator(
            final String source,
            final String component,
            final Long batchSize,
            final Long runFromEventNumber,
            final ProcessedEventTrackingRepository processedEventTrackingRepository) {
        super(MAX_VALUE, ORDERED);
        this.source = source;
        this.component = component;

        this.batchSize = batchSize;
        this.runFromEventNumber = runFromEventNumber;
        this.processedEventTrackingRepository = processedEventTrackingRepository;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super ProcessedEvent> nextProcessedEventConsumer) {

        // first time through - currentEventNumber will be -1, so get the first list of processed events
        if (currentEventNumber < 0) {

            if(LOGGER.isInfoEnabled()) {
                LOGGER.info("Making first fetch of processed events table to load {} events into memory ", batchSize);
            }
            final List<ProcessedEvent> processedEvents = processedEventTrackingRepository.getProcessedEventsInBatchesInDescendingOrder(
                    runFromEventNumber,
                    MAX_VALUE,
                    batchSize,
                    source,
                    component);

            if(LOGGER.isInfoEnabled()) {
                LOGGER.info("Fetched List of {} ProcessedEvents", processedEvents.size());
            }
            processedEventsIterator = processedEvents.iterator();
        }

        // normal iteration over processed events. will set the next processed event to be
        // returned and return true (as in we have another processed event)
        if (processedEventsIterator.hasNext()) {
            final ProcessedEvent nextProcessedEvent = processedEventsIterator.next();
            currentEventNumber = nextProcessedEvent.getPreviousEventNumber();

            nextProcessedEventConsumer.accept(nextProcessedEvent);

            return true;
        }

        // if there are no events left in the process event iterator, yet we haven't reached the final
        // event (event number 1), then fetch the next list of processed events as an iterator
        if (currentEventNumber > 0) {
            if(LOGGER.isInfoEnabled()) {
                LOGGER.info("Making fetch of processed events table to load {} events into memory ", batchSize);
            }
            final List<ProcessedEvent> processedEvents = processedEventTrackingRepository.getProcessedEventsInBatchesInDescendingOrder(
                    runFromEventNumber,
                    currentEventNumber + 1,
                    batchSize,
                    source,
                    component);

            if(LOGGER.isInfoEnabled()) {
                LOGGER.info("Fetched List of {} ProcessedEvents", processedEvents.size());
            }

            processedEventsIterator = processedEvents.iterator();

            if (processedEventsIterator.hasNext()) {
                final ProcessedEvent nextProcessedEvent = processedEventsIterator.next();
                currentEventNumber = nextProcessedEvent.getPreviousEventNumber();

                nextProcessedEventConsumer.accept(nextProcessedEvent);
            }  else {
                return false;
            }

            return true;
        }

        // if the current event number is now zero we've reached the end of the stream of events
        // so return false
        if(LOGGER.isInfoEnabled()) {
            LOGGER.info("All processed events fetched. No more events to process");
        }

        return false;
    }
}
