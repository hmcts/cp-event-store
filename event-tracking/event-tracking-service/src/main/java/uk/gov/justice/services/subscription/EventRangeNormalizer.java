package uk.gov.justice.services.subscription;

import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.LinkedList;
import java.util.List;

import jakarta.inject.Inject;

public class EventRangeNormalizer {

    @Inject
    private RangeNormalizationCalculator rangeNormalizationCalculator;

    @Inject
    private PublishedEventReadConfiguration publishedEventReadConfiguration;

    public List<MissingEventRange> normalize(final LinkedList<MissingEventRange> missingEventRanges) {

        return missingEventRanges.stream()
                .flatMap(missingEventRange -> rangeNormalizationCalculator.asStreamOfNormalizedRanges(
                        missingEventRange,
                        publishedEventReadConfiguration.getRangeNormalizationMaxSize()))
                .collect(toList());
    }
}
