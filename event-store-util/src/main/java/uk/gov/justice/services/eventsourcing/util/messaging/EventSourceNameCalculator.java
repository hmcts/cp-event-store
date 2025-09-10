package uk.gov.justice.services.eventsourcing.util.messaging;

import static org.apache.commons.lang3.StringUtils.substringBefore;

import uk.gov.justice.services.messaging.JsonEnvelope;

public class EventSourceNameCalculator {

    public String getSource(final JsonEnvelope event) {
        return getSource(event.metadata().name());
    }

    public String getSource(final String name) {
        return substringBefore(name, ".");
    }
}
