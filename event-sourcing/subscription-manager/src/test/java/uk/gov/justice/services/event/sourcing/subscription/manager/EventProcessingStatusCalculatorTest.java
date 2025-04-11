package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_IS_OBSOLETE;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.SHOULD_BUFFER_EVENT;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.SHOULD_PROCESS_EVENT;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventProcessingStatusCalculatorTest {

    @InjectMocks
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;


    @Test
    public void shouldProcessEventIfIncomingPositionIsOneGreaterThanCurrentStreamPosition() throws Exception {

        final StreamPositions streamPositions_1 = mock(StreamPositions.class);
        when(streamPositions_1.currentStreamPosition()).thenReturn(1L);
        when(streamPositions_1.incomingEventPosition()).thenReturn(2L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_1), is(SHOULD_PROCESS_EVENT));

        final StreamPositions streamPositions_2 = mock(StreamPositions.class);
        when(streamPositions_2.currentStreamPosition()).thenReturn(2L);
        when(streamPositions_2.incomingEventPosition()).thenReturn(3L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_2), is(SHOULD_PROCESS_EVENT));

        final StreamPositions streamPositions_3 = mock(StreamPositions.class);
        when(streamPositions_3.currentStreamPosition()).thenReturn(23L);
        when(streamPositions_3.incomingEventPosition()).thenReturn(24L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_3), is(SHOULD_PROCESS_EVENT));
    }

    @Test
    public void shouldBufferEventIfIncomingPositionIsMoreThanOneGreaterThanCurrentStreamPosition() throws Exception {

        final StreamPositions streamPositions_1 = mock(StreamPositions.class);
        when(streamPositions_1.currentStreamPosition()).thenReturn(1L);
        when(streamPositions_1.incomingEventPosition()).thenReturn(3L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_1), is(SHOULD_BUFFER_EVENT));

        final StreamPositions streamPositions_2 = mock(StreamPositions.class);
        when(streamPositions_2.currentStreamPosition()).thenReturn(2L);
        when(streamPositions_2.incomingEventPosition()).thenReturn(8L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_2), is(SHOULD_BUFFER_EVENT));
    }

    @Test
    public void shouldIgnoreTheEventIfTheIncomingPositionIsLessThanOrEqualToTheCurrentStreamPosition() throws Exception {

        final StreamPositions streamPositions_1 = mock(StreamPositions.class);
        when(streamPositions_1.currentStreamPosition()).thenReturn(10L);
        when(streamPositions_1.incomingEventPosition()).thenReturn(10L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_1), is(EVENT_IS_OBSOLETE));

        final StreamPositions streamPositions_2 = mock(StreamPositions.class);
        when(streamPositions_2.currentStreamPosition()).thenReturn(12L);
        when(streamPositions_2.incomingEventPosition()).thenReturn(11L);

        assertThat(eventProcessingStatusCalculator.calculateProcessingStatus(streamPositions_2), is(EVENT_IS_OBSOLETE));
    }
}