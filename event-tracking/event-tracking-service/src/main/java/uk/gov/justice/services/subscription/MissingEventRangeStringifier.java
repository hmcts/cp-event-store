package uk.gov.justice.services.subscription;

import static java.lang.Math.min;

import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.List;

import jakarta.inject.Inject;

public class MissingEventRangeStringifier {

    @Inject
    private ProcessedEventStreamerConfiguration processedEventStreamerConfiguration;

    public String createMissingEventRangeStringFrom(final List<MissingEventRange> missingEventRanges) {

        final int maxNumberToLog = maxNumberToLog(missingEventRanges);

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Missing Event Ranges: [\n");
        for (int i = 0; i < maxNumberToLog; i++) {
            if (i > 0) {
                stringBuilder.append(",\n");
            }
            stringBuilder.append(missingEventRanges.get(i));
        }

        if (maxNumberToLog != missingEventRanges.size()) {
            stringBuilder.append("\n<...further messing events not logged...>");
        }

        return stringBuilder.append("\n]").toString();
    }

    private int maxNumberToLog(final List<MissingEventRange> missingEventRanges) {

        return min(
                missingEventRanges.size(),
                processedEventStreamerConfiguration.getMaxNumberOfMissingEventRangesToLog()
        );
    }
}
