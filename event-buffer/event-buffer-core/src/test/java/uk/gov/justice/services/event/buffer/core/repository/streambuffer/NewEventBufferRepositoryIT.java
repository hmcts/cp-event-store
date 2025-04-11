package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewEventBufferRepositoryIT {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private NewEventBufferRepository newEventBufferRepository;


    @Test
    public void shouldInsertFindFirstAndRemoveEventFromEventBufferTable() throws Exception {

        final ZonedDateTime now = new UtcClock().now();
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(clock.now()).thenReturn(now);

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final EventBufferEvent eventBufferEvent_1 = new EventBufferEvent(
                streamId,
                1L,
                "some-event-json-1",
                source,
                component,
                clock.now()
        );
        final EventBufferEvent eventBufferEvent_2 = new EventBufferEvent(
                streamId,
                2L,
                "some-event-json-2",
                source,
                component,
                clock.now()
        );
        final EventBufferEvent eventBufferEvent_3 = new EventBufferEvent(
                streamId,
                3L,
                "some-event-json-3",
                source,
                component,
                clock.now()
        );

        newEventBufferRepository.buffer(eventBufferEvent_1);
        newEventBufferRepository.buffer(eventBufferEvent_3);
        newEventBufferRepository.buffer(eventBufferEvent_2);

        assertThat(newEventBufferRepository.findNextForStream(streamId, source, component), is(of(eventBufferEvent_1)));
        newEventBufferRepository.removeFromBuffer(eventBufferEvent_1);

        assertThat(newEventBufferRepository.findNextForStream(streamId, source, component), is(of(eventBufferEvent_2)));
        newEventBufferRepository.removeFromBuffer(eventBufferEvent_2);

        assertThat(newEventBufferRepository.findNextForStream(streamId, source, component), is(of(eventBufferEvent_3)));
        newEventBufferRepository.removeFromBuffer(eventBufferEvent_3);

        assertThat(newEventBufferRepository.findNextForStream(streamId, source, component), is(empty()));
    }
}