package uk.gov.justice.services.subscription;

import static javax.transaction.Transactional.TxType.NEVER;

import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class MissingEventRangeFinder {

    private static final long FIRST_POSSIBLE_EVENT_NUMBER = 0L;

    @Inject
    private ProcessedEventTrackingRepository processedEventTrackingRepository;

    @Inject
    private ProcessedEventStreamer processedEventStreamer;

    @Transactional(NEVER)
    public LinkedList<MissingEventRange> getRangesOfMissingEvents(
            final String eventSourceName,
            final String componentName,
            final Long runFromEventNumber,
            final Long highestPublishedEventNumber) {

        final EventNumberAccumulator eventNumberAccumulator = new EventNumberAccumulator();

        final Optional<ProcessedEvent> latestProcessedEvent = processedEventTrackingRepository.getLatestProcessedEvent(eventSourceName, componentName);

        if (latestProcessedEvent.isPresent()) {
            notSeenEventsRange(latestProcessedEvent.get().getPreviousEventNumber(), highestPublishedEventNumber, eventNumberAccumulator);
        } else {
            notSeenEventsRange(1L, highestPublishedEventNumber, eventNumberAccumulator);
        }

        try (final Stream<ProcessedEvent> allProcessedEventsStream = processedEventStreamer.getProcessedEventStream(
                eventSourceName,
                componentName,
                runFromEventNumber)) {
            allProcessedEventsStream.forEach(
                    processedEventTrackItem -> findMissingRange(processedEventTrackItem, eventNumberAccumulator)
            );
        }

        if (eventNumberAccumulator.isInitialised() &&
            eventNumberAccumulator.getLastPreviousEventNumber() != runFromEventNumber -1) {
            eventNumberAccumulator.addRangeFrom(runFromEventNumber - 1);
        }

        return eventNumberAccumulator.getMissingEventRanges();
    }

    private void findMissingRange(final ProcessedEvent processedEvent, final EventNumberAccumulator eventNumberAccumulator) {

        final long currentEventNumber = processedEvent.getEventNumber();
        final long currentPreviousEventNumber = processedEvent.getPreviousEventNumber();

        if (eventNumberAccumulator.isInitialised() && eventNumberAccumulator.getLastPreviousEventNumber() != currentEventNumber) {
            eventNumberAccumulator.addRangeFrom(currentEventNumber);
        }

        eventNumberAccumulator.set(currentPreviousEventNumber, currentEventNumber);
    }

    private void notSeenEventsRange(
            final long currentPreviousEventNumber,
            final long highestExistingEventNumber,
            final EventNumberAccumulator eventNumberAccumulator) {

        final long highestExclusiveEventNumber = highestExistingEventNumber + 1;

        if (eventNumberAccumulator.isInitialised() && eventNumberAccumulator.getLastPreviousEventNumber() != highestExclusiveEventNumber) {
            eventNumberAccumulator.addRangeFrom(highestExclusiveEventNumber);
        }

        eventNumberAccumulator.set(currentPreviousEventNumber, highestExclusiveEventNumber);
    }
}
