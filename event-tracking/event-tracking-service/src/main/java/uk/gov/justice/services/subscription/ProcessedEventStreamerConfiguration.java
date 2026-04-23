package uk.gov.justice.services.subscription;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;

import jakarta.inject.Inject;

public class ProcessedEventStreamerConfiguration {

    @Inject
    @GlobalValue(key = "catchup.fetch.processed.event.batch.size", defaultValue = "100000")
    private String processedEventFetchBatchSize;
    
    @Inject
    @GlobalValue(key = "catchup.max.number.of.missing.event.ranges.to.log", defaultValue = "100")
    private String maxNumberOfMissingEventRangesToLog;

    public Long getProcessedEventFetchBatchSize() {
        return parseLong(processedEventFetchBatchSize);
    }

    public Integer getMaxNumberOfMissingEventRangesToLog() {
        return parseInt(maxNumberOfMissingEventRangesToLog);
    }
}
