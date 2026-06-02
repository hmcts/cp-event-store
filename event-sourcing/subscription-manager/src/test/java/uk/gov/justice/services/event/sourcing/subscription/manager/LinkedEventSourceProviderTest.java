package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.EventSourceNameQualifier;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventSourceProviderTest {

    @Mock
    private Instance<LinkedEventSource> publishedEventSources;

    @InjectMocks
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Test
    public void shouldGetTheCorrectEventSourceByName() throws Exception {

        final String eventSourceName = "eventSourceName";

        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);

        final EventSourceNameQualifier eventSourceNameQualifier = new EventSourceNameQualifier(eventSourceName);

        final Instance<LinkedEventSource> publishedEventSourceInstance = mock(Instance.class);
        when(publishedEventSources.select(eventSourceNameQualifier)).thenReturn(publishedEventSourceInstance);
        when(publishedEventSourceInstance.get()).thenReturn(linkedEventSource);

        assertThat(linkedEventSourceProvider.getLinkedEventSource(eventSourceName), is(linkedEventSource));
    }
}
