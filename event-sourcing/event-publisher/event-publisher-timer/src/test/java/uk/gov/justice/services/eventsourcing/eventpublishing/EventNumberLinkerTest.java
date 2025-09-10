package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkableEventDetails;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.MetadataBuilder;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import java.util.UUID;

import javax.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventNumberLinkerTest {

    @Mock
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Mock
    private MetadataEventNumberUpdater metadataEventNumberUpdater;

    @Mock
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;

    @Mock
    private StringToJsonObjectConverter stringToJsonObjectConverter;

    @InjectMocks
    private EventNumberLinker eventNumberLinker;

    @Test
    public void shouldFindPreviousEventNumberUpdateEventMetadataJsonAddPreviousEventNumberToEventLogAndAddToPublishQueue() throws Exception {

        final UUID eventId = randomUUID();
        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;
        final String metadataString = "some-metadata-json";
        final String updatedMetadataJson = "updated-metadata-json";
        final JsonObject metadataJsonObject = mock(JsonObject.class);
        final JsonObject updatedMetadataJsonObject = mock(JsonObject.class);
        final Metadata metadata = mock(Metadata.class);
        final Metadata updatedMetadata = mock(Metadata.class);

        final LinkableEventDetails linkableEventDetails = new LinkableEventDetails(
                eventId,
                eventNumber,
                metadataString
        );

        final MetadataBuilder metadataBuilder = mock(MetadataBuilder.class);

        when(linkEventsInEventLogDatabaseAccess.findNextUnlinkedEvent()).thenReturn(of(linkableEventDetails));
        when(linkEventsInEventLogDatabaseAccess.findNextUnlinkedPreviousEventNumber()).thenReturn(previousEventNumber);
        when(stringToJsonObjectConverter.convert(metadataString)).thenReturn(metadataJsonObject);

        when(defaultJsonEnvelopeProvider.metadataFrom(metadataJsonObject)).thenReturn(metadataBuilder);
        when(metadataBuilder.build()).thenReturn(metadata);
        when(metadataEventNumberUpdater.updateMetadataJson(
                metadata,
                previousEventNumber,
                eventNumber)).thenReturn(updatedMetadata);
        when(updatedMetadata.asJsonObject()).thenReturn(updatedMetadataJsonObject);
        when(updatedMetadataJsonObject.toString()).thenReturn(updatedMetadataJson);

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(true));

        final InOrder inOrder = inOrder(linkEventsInEventLogDatabaseAccess);

        inOrder.verify(linkEventsInEventLogDatabaseAccess).linkEventInEventLogTable(
                eventId,
                previousEventNumber,
                updatedMetadataJson);

        inOrder.verify(linkEventsInEventLogDatabaseAccess).insertLinkedEventIntoPublishQueue(eventId);
    }

    @Test
    public void shouldDoNothingIfNoUnlinkedEventsFound() throws Exception {

        when(linkEventsInEventLogDatabaseAccess.findNextUnlinkedEvent()).thenReturn(empty());

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        verifyNoMoreInteractions(linkEventsInEventLogDatabaseAccess);
        verifyNoInteractions(stringToJsonObjectConverter);
        verifyNoInteractions(defaultJsonEnvelopeProvider);
        verifyNoInteractions(metadataEventNumberUpdater);
    }
}