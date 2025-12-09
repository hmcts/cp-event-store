package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.util;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class EventFactory {

    private final int numberOfStreams;
    private final int numberOfUniqueEventNames;

    private final UtcClock clock = new UtcClock();

    private final Random random = new Random();
    private final Map<UUID, List<PublishedEvent>> eventsByStream = new HashMap<>();

    public EventFactory(final int numberOfStreams, final int numberOfUniqueEventNames) {
        this.numberOfStreams = numberOfStreams;
        this.numberOfUniqueEventNames = numberOfUniqueEventNames;
    }

    public List<PublishedEvent> generateEvents(final int numberOfEventsToCreate) {

        final List<UUID> streamIds = generateStreamIds();
        final List<String> eventNames = generateEventNames();

        streamIds.forEach(streamId -> eventsByStream.put(streamId, new ArrayList<>()));

        return IntStream.range(0, numberOfEventsToCreate)
                .mapToObj(eventNumber -> generateEnvelope(streamIds, eventNames, eventNumber + 1))
                .collect(toList());
    }

    private List<UUID> generateStreamIds() {
        return range(0, numberOfStreams)
                .mapToObj(index -> randomUUID())
                .collect(toList());
    }

    private List<String> generateEventNames() {
        return range(0, numberOfUniqueEventNames)
                .mapToObj(index -> "context.event_" + (index + 1))
                .collect(toList());
    }

    private UUID getARandomStreamId(final List<UUID> streamIds) {

        final int index = random.nextInt(streamIds.size());

        return streamIds.get(index);
    }

    private String getARandomEventName(final List<String> eventNames) {

        final int index = random.nextInt(eventNames.size());

        return eventNames.get(index);
    }

    private PublishedEvent generateEnvelope(final List<UUID> streamIds, final List<String> eventNames, final int eventNumber) {

        final UUID streamId = getARandomStreamId(streamIds);
        final String eventName = getARandomEventName(eventNames);

        final List<PublishedEvent> jsonEnvelopesByStream = eventsByStream.get(streamId);
        final int version = jsonEnvelopesByStream.size() + 1;

        final PublishedEvent publishedEvent = generateJsonEnvelope(streamId, eventName, version, eventNumber);

        jsonEnvelopesByStream.add(publishedEvent);

        return publishedEvent;
    }

    private PublishedEvent generateJsonEnvelope(final UUID streamId, final String eventName, final long positionInStream, final long eventNumber) {
        final UUID id = randomUUID();
        final Metadata metadata = JsonEnvelope.metadataBuilder()
                .withId(id)
                .withName(eventName)
                .withStreamId(streamId)
                .withPosition(positionInStream)
                .withSource("test_source")
                .withEventNumber(eventNumber).build();

        final String metadataJson = metadata.asJsonObject().toString();

        return new PublishedEvent(
                id,
                streamId,
                positionInStream,
                eventName,
                metadataJson,
                getJsonBuilderFactory().createObjectBuilder().build().toString(),
                clock.now(),
                eventNumber,
                eventNumber - 1
        );
    }
}
