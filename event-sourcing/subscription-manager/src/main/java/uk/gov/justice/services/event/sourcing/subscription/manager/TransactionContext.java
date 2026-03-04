package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.sql.Connection;
import java.util.UUID;

/**
 * Carries the connection and advisory lock state through the event processing lifecycle.
 * Created by {@link TransactionHandler#acquireConnection()}, cleaned up by
 * {@link TransactionHandler#releaseContext(TransactionContext)}.
 */
public class TransactionContext {

    private final Connection jtaHandle;
    private final Connection physicalConnection;
    private UUID lockedStreamId;

    TransactionContext(final Connection jtaHandle, final Connection physicalConnection) {
        this.jtaHandle = jtaHandle;
        this.physicalConnection = physicalConnection;
    }

    public Connection physicalConnection() {
        return physicalConnection;
    }

    Connection jtaHandle() {
        return jtaHandle;
    }

    UUID lockedStreamId() {
        return lockedStreamId;
    }

    void setLockedStreamId(final UUID lockedStreamId) {
        this.lockedStreamId = lockedStreamId;
    }
}