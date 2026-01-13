package uk.gov.justice.eventsourcing.discovery.dataaccess;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

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
                randomUUID(),
                23L,
                new UtcClock().now()
                );
        final EventSubscriptionStatus eventSubscriptionStatus_2 = new EventSubscriptionStatus(
                source_2,
                component_2,
                randomUUID(),
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
                randomUUID(),
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
                randomUUID(),
                23L,
                new UtcClock().now().minusMinutes(5)
        );
        final EventSubscriptionStatus eventSubscriptionStatus_2 = new EventSubscriptionStatus(
                "some-source_2",
                "some-component_2",
                randomUUID(),
                234L,
                new UtcClock().now().minusMinutes(2)
        );
        final EventSubscriptionStatus eventSubscriptionStatus_3 = new EventSubscriptionStatus(
                "some-source_3",
                "some-component_3",
                randomUUID(),
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
}