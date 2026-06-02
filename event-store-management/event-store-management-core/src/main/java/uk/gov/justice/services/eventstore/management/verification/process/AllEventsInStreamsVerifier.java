package uk.gov.justice.services.eventstore.management.verification.process;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.success;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.warning;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jakarta.inject.Inject;

import org.slf4j.Logger;

public class AllEventsInStreamsVerifier implements Verifier {

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private ActiveStreamIdProvider activeStreamIdProvider;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying all streams contain at least one event...");

        final Set<UUID> allActiveStreamIds = activeStreamIdProvider.getAllActiveStreamIds();

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT DISTINCT stream_id from event_log");
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                final UUID streamId = (UUID) resultSet.getObject("stream_id");
                allActiveStreamIds.remove(streamId);
            }

            if (allActiveStreamIds.isEmpty()) {
                return singletonList(success("All streams have at least one event"));
            }

            return singletonList(warning(format(
                    "The following %d streams in the stream_status table have no events: %s",
                    allActiveStreamIds.size(),
                    allActiveStreamIds)
            ));

        } catch (SQLException e) {
            throw new CatchupVerificationException("Failed to get the stream ids from event_log", e);
        }
    }
}
