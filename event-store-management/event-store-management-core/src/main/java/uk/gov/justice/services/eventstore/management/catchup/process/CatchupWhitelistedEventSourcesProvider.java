package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import uk.gov.justice.services.jdbc.persistence.InitialContextFactory;

import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;
import javax.naming.NamingException;

import org.slf4j.Logger;

public class CatchupWhitelistedEventSourcesProvider {

    private static final String CATCHUP_EVENT_SOURCE_WHITELIST_JNDI_NAME = "java:global/catchup.event.source.whitelist";
    private static final String ALLOW_ALL = "ALLOW_ALL";

    @Inject
    private InitialContextFactory initialContextFactory;

    @Inject
    private Logger logger;

    public Optional<List<String>> getWhiteListedEventSources() {

        try {
            final String catchupEventSourceWhitelist = (String) initialContextFactory
                    .create()
                    .lookup(CATCHUP_EVENT_SOURCE_WHITELIST_JNDI_NAME);
            if (! ALLOW_ALL.equals(catchupEventSourceWhitelist)) {
                final String[] split = catchupEventSourceWhitelist.split(",");
                final List<String> whitelistedEventSources = stream(split).map(String::trim).toList();
                return of(whitelistedEventSources);
            }
        } catch (final NamingException e) {
            logger.warn(format("No whitelisted catchup event-sources found: " +
                    "allowing catchup for all event-sources. Lookup made using JNDI name '%s'. " +
                    " Lookup error message: '%s'", CATCHUP_EVENT_SOURCE_WHITELIST_JNDI_NAME, e.getMessage()));
        }

        return empty();
    }
}
