package uk.gov.justice.services.event.sourcing.subscription.manager;

import org.slf4j.MDC;

public class MdcWrapper {

    public void put(final String key, final String value) {
        MDC.put(key, value);
    }

    public void clear() {
        MDC.clear();
    }
}
