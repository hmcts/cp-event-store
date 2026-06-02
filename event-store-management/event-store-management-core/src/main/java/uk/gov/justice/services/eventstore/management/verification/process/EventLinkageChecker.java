package uk.gov.justice.services.eventstore.management.verification.process;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.error;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.success;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

public class EventLinkageChecker {

    @Inject
    private EventLinkageErrorMessageGenerator eventLinkageErrorMessageGenerator;

    public List<VerificationResult> verifyEventNumbersAreLinkedCorrectly(
            final LinkedEventNumberTable linkedEventNumberTable,
            final DataSource dataSource) {

        final String query = format("SELECT event_number, previous_event_number FROM %s ORDER BY event_number", linkedEventNumberTable.getTableName());

        final List<VerificationResult> errors = new ArrayList<>();

        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(query);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            int lastEventNumber = 0;
            int count = 0;
            while (resultSet.next()) {
                final int eventNumber = resultSet.getInt("event_number");
                final int previousEventNumber = resultSet.getInt("previous_event_number");

                if (previousEventNumber != lastEventNumber) {

                    final String errorMessage = eventLinkageErrorMessageGenerator.generateErrorMessage(
                            previousEventNumber,
                            eventNumber,
                            lastEventNumber,
                            linkedEventNumberTable);

                    errors.add(error(errorMessage));
                }

                lastEventNumber = eventNumber;
                count++;
            }

            if (errors.isEmpty()) {

                final String message = format(
                        "All %d events in the %s table are correctly linked",
                        count,
                        linkedEventNumberTable.getTableName());

                return singletonList(success(message));
            }

            return errors;

        } catch (final SQLException e) {
            throw new CatchupVerificationException(format("Failed to get event numbers from %s table", linkedEventNumberTable.getTableName()), e);
        }
    }
}
