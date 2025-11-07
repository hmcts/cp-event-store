package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import javax.inject.Inject;
import uk.gov.justice.services.common.configuration.Value;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class EventLinkingWorkerConfig {

    private static final String DEFAULT_TRANSACTION_TIMEOUT_SECONDS = "5";

    @Inject
    @Value(key = "event.linking.worker.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.time.between.runs.milliseconds", defaultValue = "5")
    private String timeBetweenRunsMilliseconds;

    @Inject
    @Value(key = "event.publishing.add.event.to.published.event.table.on.publish", defaultValue = "true")
    private String insertEventIntoPublishedEventTable;

    @Inject
    @Value(key = "event.linking.worker.transaction.timeout.seconds", defaultValue = DEFAULT_TRANSACTION_TIMEOUT_SECONDS)
    private String transactionTimeoutSeconds;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimeBetweenRunsMilliseconds() {
        return parseLong(timeBetweenRunsMilliseconds);
    }

    public boolean shouldAlsoInsertEventIntoPublishedEventTable() {
        return parseBoolean(insertEventIntoPublishedEventTable);
    }

    public int getTransactionTimeoutSeconds() {
        return parseInt(transactionTimeoutSeconds);
    }
}
