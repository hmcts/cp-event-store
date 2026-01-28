package uk.gov.justice.subscription;

import java.io.Serializable;

public record SourceComponentPair(String source, String component) implements Serializable {
}
