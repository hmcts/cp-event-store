package uk.gov.justice.eventsourcing.discovery.dataaccess;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSubscriptionStatusRepositoryIT {

    private final FrameworkTestDataSourceFactory frameworkTestDataSourceFactory = new FrameworkTestDataSourceFactory();

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @BeforeEach
    public void cleanEventTrackingTable() {
        new DatabaseCleaner().cleanViewStoreTables("framework", "event_subscription_status");
    }

    @Test
    public void shouldSaveAndFind() throws Exception {
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(frameworkTestDataSourceFactory.createViewStoreDataSource());

        final String source_1 = "some-source_1";
        final String component_1 = "some-component_1";
        final String source_2 = "some-source_2";
        final String component_2 = "some-component_2";
        final EventSubscriptionStatus eventSubscriptionStatus_1 = new EventSubscriptionStatus(
                source_1,
                component_1,
                of(randomUUID()),
                23L,
                new UtcClock().now()
                );
        final EventSubscriptionStatus eventSubscriptionStatus_2 = new EventSubscriptionStatus(
                source_2,
                component_2,
                of(randomUUID()),
                234L,
                new UtcClock().now()
                );

        eventSubscriptionStatusRepository.save(eventSubscriptionStatus_1);
        eventSubscriptionStatusRepository.save(eventSubscriptionStatus_2);

        final List<EventSubscriptionStatus> foundEventDiscoveryDetails = eventSubscriptionStatusRepository.findAll();

        assertThat(foundEventDiscoveryDetails.size(), is(2));
        assertThat(foundEventDiscoveryDetails.get(0), is(eventSubscriptionStatus_1));
        assertThat(foundEventDiscoveryDetails.get(1), is(eventSubscriptionStatus_2));
    }

    @Test
    public void shouldUpsertDataWhenSaving() throws Exception {

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(frameworkTestDataSourceFactory.createViewStoreDataSource());

        final long firstLatestKnownPosition = 23L;
        final long updatedLatestKnownPosition = 27L;
        final EventSubscriptionStatus eventSubscriptionStatus = new EventSubscriptionStatus(
                "some-source",
                "some-component",
                of(randomUUID()),
                firstLatestKnownPosition,
                new UtcClock().now().minusMinutes(2)
        );

        assertThat(eventSubscriptionStatusRepository.findAll(), is(emptyList()));

        eventSubscriptionStatusRepository.save(eventSubscriptionStatus);
        final List<EventSubscriptionStatus> eventSubscriptionStatuses = eventSubscriptionStatusRepository.findAll();
        assertThat(eventSubscriptionStatuses.size(), is(1));
        assertThat(eventSubscriptionStatuses.get(0), is(eventSubscriptionStatus));


        final EventSubscriptionStatus eventSubscriptionStatusUpdated = new EventSubscriptionStatus(
                eventSubscriptionStatus.source(),
                eventSubscriptionStatus.component(),
                eventSubscriptionStatus.latestEventId(),
                updatedLatestKnownPosition,
                new UtcClock().now()
        );

        eventSubscriptionStatusRepository.save(eventSubscriptionStatusUpdated);

        final List<EventSubscriptionStatus> foundEventSubscriptionStatuses = eventSubscriptionStatusRepository.findAll();

        assertThat(foundEventSubscriptionStatuses.size(), is(1));
        assertThat(foundEventSubscriptionStatuses.get(0), is(eventSubscriptionStatusUpdated));
    }

    @Test
    public void shouldFindBySourceAndComponent() throws Exception {

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(frameworkTestDataSourceFactory.createViewStoreDataSource());
        
        final EventSubscriptionStatus eventSubscriptionStatus_1 = new EventSubscriptionStatus(
                "some-source_1",
                "some-component_1",
                of(randomUUID()),
                23L,
                new UtcClock().now().minusMinutes(5)
        );
        final EventSubscriptionStatus eventSubscriptionStatus_2 = new EventSubscriptionStatus(
                "some-source_2",
                "some-component_2",
                of(randomUUID()),
                234L,
                new UtcClock().now().minusMinutes(2)
        );
        final EventSubscriptionStatus eventSubscriptionStatus_3 = new EventSubscriptionStatus(
                "some-source_3",
                "some-component_3",
                of(randomUUID()),
                897L,
                new UtcClock().now().minusMinutes(1)
        );

        eventSubscriptionStatusRepository.save(eventSubscriptionStatus_1);
        eventSubscriptionStatusRepository.save(eventSubscriptionStatus_2);
        eventSubscriptionStatusRepository.save(eventSubscriptionStatus_3);

        final Optional<EventSubscriptionStatus> eventSubscriptionStatus = eventSubscriptionStatusRepository.findBy(
                eventSubscriptionStatus_2.source(),
                eventSubscriptionStatus_2.component());

        assertThat(eventSubscriptionStatus.isPresent(), is(true));
        assertThat(eventSubscriptionStatus.get(), is(eventSubscriptionStatus_2));
    }

    @Test
    public void shouldInsertEmptyRowForSourceComponent() throws Exception {

        final String source = "some-new-source";
        final String component = "some-new-component";
        final ZonedDateTime updatedAt = new UtcClock().now();

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(frameworkTestDataSourceFactory.createViewStoreDataSource());
        when(clock.now()).thenReturn(updatedAt);

        assertThat(eventSubscriptionStatusRepository.findBy(source, component).isEmpty(), is(true));

        final int rowsUpdated = eventSubscriptionStatusRepository.insertEmptyRowFor(source, component);

        assertThat(rowsUpdated, is(1));

        final Optional<EventSubscriptionStatus> eventSubscriptionStatus = eventSubscriptionStatusRepository.findBy(
                source,
                component);

        if (eventSubscriptionStatus.isPresent()) {
            assertThat(eventSubscriptionStatus.get().source(), is(source));
            assertThat(eventSubscriptionStatus.get().component(), is(component));
            assertThat(eventSubscriptionStatus.get().latestKnownPosition(), is(-1L));
            assertThat(eventSubscriptionStatus.get().latestEventId(), is(empty()));
            assertThat(eventSubscriptionStatus.get().updatedAt(), is(updatedAt));
        } else {
            fail();
        }
    }

    @Test
    public void shouldDoNothingIfInsertingEmptyRowHasConflict() throws Exception {

        final String source = "some-new-source";
        final String component = "some-new-component";
        final ZonedDateTime updatedAt = new UtcClock().now();

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(frameworkTestDataSourceFactory.createViewStoreDataSource());
        when(clock.now()).thenReturn(updatedAt);

        assertThat(eventSubscriptionStatusRepository.findBy(source, component).isEmpty(), is(true));

        // first insert should add one row
        assertThat(eventSubscriptionStatusRepository.insertEmptyRowFor(source, component), is(1));

        // subsequent inserts should do nothing...
        assertThat(eventSubscriptionStatusRepository.insertEmptyRowFor(source, component), is(0));
        assertThat(eventSubscriptionStatusRepository.insertEmptyRowFor(source, component), is(0));
        assertThat(eventSubscriptionStatusRepository.insertEmptyRowFor(source, component), is(0));
        assertThat(eventSubscriptionStatusRepository.insertEmptyRowFor(source, component), is(0));
    }
}