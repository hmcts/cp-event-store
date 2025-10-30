package uk.gov.justice.services.subscription;

import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ProcessedEventTrackingServiceTest {

    @Mock
    private ProcessedEventTrackingRepository processedEventTrackingRepository;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Mock
    private MissingEventRangeFinder missingEventRangeFinder;

    @Mock
    private EventRangeNormalizer eventRangeNormalizer;

    @Mock
    private MissingEventRangeStringifier missingEventRangeStringifier;

    @Mock
    private Logger logger;

    @InjectMocks
    private ProcessedEventTrackingService processedEventTrackingService;

    @Test
    public void shouldStoreCurrentEventNumberLinkedToPreviousEvent() throws Exception {

        final long previousEventNumber = 23;
        final long eventNumber = 24;

        final UUID eventId = randomUUID();
        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder()
                        .withId(eventId)
                        .withName("event-name")
                        .withPreviousEventNumber(previousEventNumber)
                        .withEventNumber(eventNumber)
                        .withSource(source),
                getJsonBuilderFactory().createObjectBuilder());

        when(eventSourceNameCalculator.getSource(event)).thenReturn(source);

        processedEventTrackingService.trackProcessedEvent(event, componentName);

        verify(processedEventTrackingRepository).save(new ProcessedEvent(eventId, previousEventNumber, eventNumber, source, componentName));
    }

    @Test
    public void shouldThrowExceptionIfNoPreviousEventNumberFound() throws Exception {

        final long eventNumber = 24;

        final UUID id = fromString("8df3a64b-589e-4c55-adc1-b767ddbab42f");
        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName("event-name")
                        .withEventNumber(eventNumber)
                        .withSource(source),
                getJsonBuilderFactory().createObjectBuilder());

        try {
            processedEventTrackingService.trackProcessedEvent(event, componentName);
            fail();
        } catch (final ProcessedEventTrackingException expected) {
            assertThat(expected.getMessage(), is("Missing previous event number for event with id '8df3a64b-589e-4c55-adc1-b767ddbab42f'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNoEventNumberFound() throws Exception {

        final long previousEventNumber = 23;

        final UUID id = fromString("8df3a64b-589e-4c55-adc1-b767ddbab42f");
        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName("event-name")
                        .withPreviousEventNumber(previousEventNumber)
                        .withSource(source),
                getJsonBuilderFactory().createObjectBuilder());

        try {
            processedEventTrackingService.trackProcessedEvent(event, componentName);
            fail();
        } catch (final ProcessedEventTrackingException expected) {
            assertThat(expected.getMessage(), is("Missing event number for event with id '8df3a64b-589e-4c55-adc1-b767ddbab42f'"));
        }
    }

    @Test
    public void shouldGetTheRangesOfAllMissingEvents() throws Exception {

        final String eventSourceName = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long highestPublishedEventNumber = 10L;
        final Long runFromEventNumber = 1L;
        final String missingEventRangeString = "...some missing events...";

        final MissingEventRange missingEventRange_1 = new MissingEventRange(4L, 7L);
        final MissingEventRange missingEventRange_2 = new MissingEventRange(8L, highestPublishedEventNumber);

        final LinkedList<MissingEventRange> missingEventRangeList = new LinkedList<>(asList(missingEventRange_1, missingEventRange_2));

        when(missingEventRangeFinder.getRangesOfMissingEvents(eventSourceName, componentName, runFromEventNumber, highestPublishedEventNumber)).thenReturn(missingEventRangeList);
        when(eventRangeNormalizer.normalize(missingEventRangeList)).thenReturn(missingEventRangeList);
        when(missingEventRangeStringifier.createMissingEventRangeStringFrom(missingEventRangeList)).thenReturn(missingEventRangeString);
        when(logger.isInfoEnabled()).thenReturn(true);

        final List<MissingEventRange> missingEventRanges = processedEventTrackingService
                .getAllMissingEvents(
                        eventSourceName,
                        componentName,
                        runFromEventNumber,
                        highestPublishedEventNumber)
                .toList();

        assertThat(missingEventRanges.size(), is(2));
        assertThat(missingEventRanges.get(0), is(missingEventRange_1));
        assertThat(missingEventRanges.get(1), is(missingEventRange_2));

        verify(logger).info("Found 2 missing event ranges");
        verify(logger).info("Event ranges normalized to 2 missing event ranges");
        verify(logger).info(missingEventRangeString);
    }

    @Test
    public void shouldGetTheEventNumberOfTheLatestProcessedEvent() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final long latestEventNumber = 2384L;

        final ProcessedEvent processedEvent = mock(ProcessedEvent.class);
        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(processedEvent));
        when(processedEvent.getEventNumber()).thenReturn(latestEventNumber);

        assertThat(processedEventTrackingService.getLatestProcessedEventNumber(source, componentName), is(latestEventNumber));
    }

    @Test
    public void shouldReturnZeroAsTheEventNumberOfTheLatestProcessedEventIfNoEventsProcessedYet() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(Optional.empty());

        assertThat(processedEventTrackingService.getLatestProcessedEventNumber(source, componentName), is(0L));
    }
}
