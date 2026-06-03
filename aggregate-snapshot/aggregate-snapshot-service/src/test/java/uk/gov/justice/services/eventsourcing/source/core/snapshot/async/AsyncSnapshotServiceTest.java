package uk.gov.justice.services.eventsourcing.source.core.snapshot.async;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import uk.gov.justice.domain.aggregate.TestAggregate;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotDeleterRequestEvent;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.event.SnapshotSaverRequestEvent;

import java.util.UUID;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncSnapshotServiceTest {

    @Mock
    private Event<SnapshotSaverRequestEvent> aggregateSnapshotSaverRequestEventFirer;
    @Mock
    private Event<SnapshotDeleterRequestEvent> aggregateSnapshotDeletionRequestFirer;

    @InjectMocks
    private AsyncSnapshotService snapshotService;


    @Test
    void shouldSaveAggregateSnapshot() {
        snapshotService.saveAggregateSnapshot(UUID.randomUUID(), 1L, new TestAggregate());
        verify(aggregateSnapshotSaverRequestEventFirer).fireAsync(any(SnapshotSaverRequestEvent.class));
    }

    @Test
    void shouldRemoveAggregateSnapshot() {
        snapshotService.removeAggregateSnapshot(UUID.randomUUID(), TestAggregate.class, 1L, new UtcClock().now());
        verify(aggregateSnapshotDeletionRequestFirer).fireAsync(any(SnapshotDeleterRequestEvent.class));
    }
}