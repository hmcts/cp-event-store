package uk.gov.justice.services.eventstore.metrics.tags;

public enum TagNames {


    SOURCE_TAG_NAME("source"),
    COMPONENT_TAG_NAME("component"),
    SERVICE_TAG_NAME("service"),
    ENV_TAG_NAME("env");

    private final String tagName;

    TagNames(final String tagName) {
        this.tagName = tagName;
    }

    public String getTagName() {
        return tagName;
    }

    @Override
    public String toString() {
        return tagName;
    }
}
