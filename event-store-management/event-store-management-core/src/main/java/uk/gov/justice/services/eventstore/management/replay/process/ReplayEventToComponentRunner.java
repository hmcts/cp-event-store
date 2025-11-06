package uk.gov.justice.services.eventstore.management.replay.process;

import uk.gov.justice.services.common.util.UtcClock;

import java.util.UUID;

import javax.inject.Inject;

public class ReplayEventToComponentRunner {

    @Inject
    private EventSourceNameFinder eventSourceNameFinder;

    @Inject
    private ReplayEventToEventListenerProcessorBean replayEventToEventListenerProcessorBean;

    public void run(final UUID commandId, final UUID commandRuntimeId, final String componentName) {
        final String eventSourceName = eventSourceNameFinder.getEventSourceNameOf(componentName);

        final ReplayEventContext replayEventContext = new ReplayEventContext(commandId,
                commandRuntimeId, eventSourceName, componentName);

        replayEventToEventListenerProcessorBean.perform(replayEventContext);
    }
}
