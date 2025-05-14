package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LatestKnownPositionAndIsUpToDateUpdaterTest {

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @InjectMocks
    private LatestKnownPositionAndIsUpToDateUpdater latestKnownPositionAndIsUpToDateUpdater;

    @Test
    public void shouldUpdateLatestKnownPositionIfIncomingPositionIsGreaterThanCurrentLatestKnownPosition() throws Exception {

        final long incomingPositionInStream = 23;
        final long latestKnownStreamPosition = 22;

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";

        final StreamPositions streamPositions = mock(StreamPositions.class);


        when(streamPositions.latestKnownStreamPosition()).thenReturn(latestKnownStreamPosition);
        when(streamPositions.incomingEventPosition()).thenReturn(incomingPositionInStream);

        latestKnownPositionAndIsUpToDateUpdater.updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName
        );

        verify(newStreamStatusRepository).updateLatestKnownPositionAndIsUpToDateToFalse(streamId, source, componentName, incomingPositionInStream);
    }

    @Test
    public void shouldNotUpdateLatestKnownPositionIfIncomingPositionIsLessThanCurrentLatestKnownPosition() throws Exception {

        final long incomingPositionInStream = 23;
        final long latestKnownStreamPosition = 24;

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";

        final StreamPositions streamPositions = mock(StreamPositions.class);


        when(streamPositions.latestKnownStreamPosition()).thenReturn(latestKnownStreamPosition);
        when(streamPositions.incomingEventPosition()).thenReturn(incomingPositionInStream);

        latestKnownPositionAndIsUpToDateUpdater.updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName
        );

        verify(newStreamStatusRepository, never()).updateLatestKnownPositionAndIsUpToDateToFalse(streamId, source, componentName, incomingPositionInStream);
    }

    @Test
    public void shouldNotUpdateLatestKnownPositionIfIncomingPositionIsEqualToCurrentLatestKnownPosition() throws Exception {

        final long incomingPositionInStream = 23;
        final long latestKnownStreamPosition = 23;

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";

        final StreamPositions streamPositions = mock(StreamPositions.class);


        when(streamPositions.latestKnownStreamPosition()).thenReturn(latestKnownStreamPosition);
        when(streamPositions.incomingEventPosition()).thenReturn(incomingPositionInStream);

        latestKnownPositionAndIsUpToDateUpdater.updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName
        );

        verify(newStreamStatusRepository, never()).updateLatestKnownPositionAndIsUpToDateToFalse(streamId, source, componentName, incomingPositionInStream);
    }
}