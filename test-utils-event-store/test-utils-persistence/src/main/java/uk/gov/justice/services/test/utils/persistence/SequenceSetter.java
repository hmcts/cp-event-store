package uk.gov.justice.services.test.utils.persistence;

import static java.lang.String.format;

import uk.gov.justice.services.jdbc.persistence.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

public class SequenceSetter {

    public void setSequenceTo(final long startNumber, final String sequenceName, final DataSource dataSource) {
        try (
                final Connection connection = dataSource.getConnection();
                final PreparedStatement preparedStatement = connection.prepareStatement(
                        "ALTER SEQUENCE " + sequenceName + " RESTART WITH " + startNumber)) {

            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new DataAccessException(format("Failed to set '%s' sequence to %d", sequenceName, startNumber), e);
        }
    }

    @SuppressWarnings("SqlResolve")
    public long getCurrentSequenceValue(final String sequenceName, final DataSource dataSource)  {
        try(final Connection connection = dataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement("SELECT LAST_VALUE FROM " + sequenceName);
            final ResultSet resultSet = preparedStatement.executeQuery()) {

            resultSet.next();

            return resultSet.getLong(1);
        }catch (final SQLException e) {
            throw new DataAccessException(format("Failed to get current value from '%s' sequence", sequenceName), e);
        }
    }

    public long getNextSequenceValue(final String sequenceName, final DataSource dataSource)  {
        try(final Connection connection = dataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement("SELECT nextval('" + sequenceName + "')");
            final ResultSet resultSet = preparedStatement.executeQuery()) {

            resultSet.next();

            return resultSet.getLong(1);
        }catch (final SQLException e) {
            throw new DataAccessException(format("Failed to current value from '%s' sequence", sequenceName), e);
        }
    }
}
