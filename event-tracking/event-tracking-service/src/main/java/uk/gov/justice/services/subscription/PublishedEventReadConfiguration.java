package uk.gov.justice.services.subscription;

import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class PublishedEventReadConfiguration {

    @Inject
    @GlobalValue(key = "catchup.published.event.read.batch.size", defaultValue = "20")
    private String rangeNormalizationMaxSize;

    public Long getRangeNormalizationMaxSize() {
        return parseLong(rangeNormalizationMaxSize);
    }
}
