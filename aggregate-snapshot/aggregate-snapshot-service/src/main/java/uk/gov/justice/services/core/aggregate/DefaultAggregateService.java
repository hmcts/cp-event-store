package uk.gov.justice.services.core.aggregate;

import static java.lang.String.format;
import static uk.gov.justice.domain.annotation.Event.SYSTEM_EVENTS;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.converter.JsonObjectToObjectConverter;
import uk.gov.justice.services.core.extension.EventFoundEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.AsyncSnapshotService;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.slf4j.Logger;

/**
 * Service for replaying event streams on aggregates.
 */
@ApplicationScoped
@Alternative
@Priority(1)
public class DefaultAggregateService implements AggregateService {

    private static final long DEFAULT_SNAPSHOT_BACKGROUND_SAVING_THRESHOLD = 50000;

    @Inject
    Logger logger;

    @Inject
    JsonObjectToObjectConverter jsonObjectToObjectConverter;

    @Inject
    private AsyncSnapshotService asyncSnapshotService;

    @Inject
    @Value(key = "snapshot.background.saving.threshold", defaultValue = "" + DEFAULT_SNAPSHOT_BACKGROUND_SAVING_THRESHOLD)
    private long snapshotBackgroundSavingThreshold;

    private ConcurrentHashMap<String, Class<?>> eventMap = new ConcurrentHashMap<>();

    /**
     * Recreate an aggregate of the specified type by replaying the events from an event stream.
     *
     * @param stream the event stream to replay
     * @param clazz  the type of aggregate to recreate
     * @param <T>    the type of aggregate being recreated
     * @return the recreated aggregate
     */
    public <T extends Aggregate> T get(final EventStream stream, final Class<T> clazz) {

        try {
            logger.trace("Recreating aggregate for instance {} of aggregate type {}", stream.getId(), clazz);
            final T aggregate = clazz.newInstance();
            return applyEvents(stream.read(), aggregate);
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(format("Could not instantiate aggregate of class %s", clazz.getName()), ex);
        }
    }

    public <T extends Aggregate> T applyEvents(final Stream<JsonEnvelope> events, final T aggregate) {
        logger.trace("Apply events for aggregate: {}", aggregate.getClass());
        try (final Stream<JsonEnvelope> e1 = events) {
            aggregate.applyForEach(events.filter(e -> !e.metadata().name().startsWith(SYSTEM_EVENTS)).
                    map(ev -> trySaveSnapshotInBackground(ev, aggregate)).
                    map(this::convertEnvelopeToEvent));
            return aggregate;
        }
    }


    /**
     * Register method, invoked automatically to register all event classes into the eventMap.
     *
     * @param event identified by the framework to be registered into the event map
     */
    void register(@Observes final EventFoundEvent event) {
        logger.info("Registering event {}, {} with DefaultAggregateService", event.getEventName(), event.getClazz());
        eventMap.putIfAbsent(event.getEventName(), event.getClazz());
    }

    private Object convertEnvelopeToEvent(final JsonEnvelope event) {
        final String name = event.metadata().name();
        if (!eventMap.containsKey(name)) {
            throw new IllegalStateException(format("No event class registered for events of type %s", name));
        }

        return jsonObjectToObjectConverter.convert(event.payloadAsJsonObject(), eventMap.get(name));
    }

    private <T extends Aggregate> JsonEnvelope trySaveSnapshotInBackground(final JsonEnvelope event, final T aggregate) {
        final Long currentPositionInStream = event.metadata().position().orElse(-1L);
        final UUID streamId = event.metadata().streamId().orElse(null);

        if (needsToSaveSnapshotInBackground(currentPositionInStream, streamId)) {
            asyncSnapshotService.saveAggregateSnapshot(streamId, currentPositionInStream - 1, aggregate);
        }
        return event;
    }

    boolean needsToSaveSnapshotInBackground(final Long currentPositionInStream, final UUID streamId) {

        if (currentPositionInStream == null || streamId == null) {
            return false;
        }

        //position needs to be strictly greater than 1 as we need to go back one step in order to save
        if (currentPositionInStream <= 1) {
            return false;
        }

        final long threshold = snapshotBackgroundSavingThreshold <= 0 ? DEFAULT_SNAPSHOT_BACKGROUND_SAVING_THRESHOLD : snapshotBackgroundSavingThreshold;

        return (currentPositionInStream % threshold == 1);
    }

}
