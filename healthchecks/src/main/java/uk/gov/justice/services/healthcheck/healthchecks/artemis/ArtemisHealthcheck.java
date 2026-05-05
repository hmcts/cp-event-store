package uk.gov.justice.services.healthcheck.healthchecks.artemis;

import uk.gov.justice.services.healthcheck.api.Healthcheck;
import uk.gov.justice.services.healthcheck.api.HealthcheckResult;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;

import org.slf4j.Logger;

public class ArtemisHealthcheck implements Healthcheck {

    public static final String ARTEMIS_HEALTHCHECK_NAME = "artemis-healthcheck";

    @Inject
    private Logger logger;

    @Inject
    private ConnectionFactory connectionFactory;

    @Inject
    private JmsDestinationsVerifier jmsDestinationsVerifier;

    @Override
    public String getHealthcheckName() {
        return ARTEMIS_HEALTHCHECK_NAME;
    }

    @Override
    public String healthcheckDescription() {
        return "Checks connectivity to the artemis broker and that all required destinations are available";
    }

    @Override
    public HealthcheckResult runHealthcheck() {
            try(final var connection = connectionFactory.createConnection()) {
                connection.start();
                try(final var session = connection.createSession()) {
                    jmsDestinationsVerifier.verify(session);
                    return HealthcheckResult.success();
                }
            }
        catch (final DestinationNotFoundException e) {
            return HealthcheckResult.failure(String.format("Exception thrown while verifying destination(s): %s", e.getMessage()));
        } catch (final JMSException e) {
            this.logger.error("Healthcheck for artemis failed.", e);
            return HealthcheckResult.failure(String.format("Exception thrown while accessing artemis broker. %s: %s", e.getClass().getName(), e.getMessage()));
        }
    }
}
