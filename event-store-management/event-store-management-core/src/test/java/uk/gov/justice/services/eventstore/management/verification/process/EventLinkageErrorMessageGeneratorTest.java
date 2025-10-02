package uk.gov.justice.services.eventstore.management.verification.process;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.eventstore.management.verification.process.LinkedEventNumberTable.EVENT_LOG;
import static uk.gov.justice.services.eventstore.management.verification.process.LinkedEventNumberTable.PROCESSED_EVENT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkageErrorMessageGeneratorTest {

    @InjectMocks
    private EventLinkageErrorMessageGenerator eventLinkageErrorMessageGenerator;

    @Test
    public void shouldGenerateTheCorrectErrorMessageForPublishedEvent() throws Exception {

        final int previousEventNumber = 23;
        final int currentEventNumber = 24;
        final int lastEvenNumber = 13;

        final String errorMessage = eventLinkageErrorMessageGenerator.generateErrorMessage(
                previousEventNumber,
                currentEventNumber,
                lastEvenNumber,
                EVENT_LOG);

        assertThat(errorMessage, is("Events incorrectly linked in event_log table: " +
                "Event with event number 24 is linked to previous event number 23 " +
                "whereas it should be 13"));
    }

    @Test
    public void shouldGenerateTheCorrectErrorMessageForProcessedEvent() throws Exception {

        final int previousEventNumber = 41;
        final int currentEventNumber = 42;
        final int lastEvenNumber = 23;

        final String errorMessage = eventLinkageErrorMessageGenerator.generateErrorMessage(
                previousEventNumber,
                currentEventNumber,
                lastEvenNumber,
                PROCESSED_EVENT);

        assertThat(errorMessage, is("Events missing from processed_event table: " +
                "Event with event_number 42 has a previous_event_number of 41, " +
                "but the event in the previous row in the database has an event_number of 23"));
    }
}
