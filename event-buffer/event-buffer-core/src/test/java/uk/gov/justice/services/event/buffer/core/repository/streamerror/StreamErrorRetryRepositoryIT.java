package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry.StreamErrorRetryBuilder.from;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorRetryRepositoryIT {

    private static final String CONTEXT_NAME = "framework";

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private StreamErrorRetryRepository streamErrorRetryRepository;

    @BeforeEach
    public void cleanDatabase() {
        new DatabaseCleaner().cleanViewStoreTables(CONTEXT_NAME, "stream_error_retry");
    }

    @Test
    public void shouldUpsertStreamErrorRetry() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(CONTEXT_NAME);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamErrorRetryRepository.findAll(), is(emptyList()));

        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                randomUUID(),
                "some-source",
                "some-component",
                new UtcClock().now(),
                23L,
                new UtcClock().now().plusMinutes(20)
        );

        streamErrorRetryRepository.upsert(streamErrorRetry);

        final List<StreamErrorRetry> streamErrorRetries = streamErrorRetryRepository.findAll();

        assertThat(streamErrorRetries.size(), is(1));
        assertThat(streamErrorRetries.get(0).streamId(), is(streamErrorRetry.streamId()));
        assertThat(streamErrorRetries.get(0).source(), is(streamErrorRetry.source()));
        assertThat(streamErrorRetries.get(0).component(), is(streamErrorRetry.component()));
        assertThat(streamErrorRetries.get(0).occurredAt(), is(streamErrorRetry.occurredAt()));
        assertThat(streamErrorRetries.get(0).retryCount(), is(streamErrorRetry.retryCount()));
        assertThat(streamErrorRetries.get(0).nextRetryTime(), is(streamErrorRetry.nextRetryTime()));

        final StreamErrorRetry updatedStreamErrorRetry = from(streamErrorRetry)
                .incrementRetryCount()
                .withNextRetryTime(new UtcClock().now().plusMinutes(2))
                .withOccurredAt(new UtcClock().now().minusMinutes(1))
                .build();

        assertThat(updatedStreamErrorRetry.retryCount(), is(streamErrorRetry.retryCount() + 1));

        streamErrorRetryRepository.upsert(updatedStreamErrorRetry);

        final List<StreamErrorRetry> updatedStreamErrorRetries = streamErrorRetryRepository.findAll();

        assertThat(updatedStreamErrorRetries.size(), is(1));
        assertThat(updatedStreamErrorRetries.get(0).streamId(), is(updatedStreamErrorRetry.streamId()));
        assertThat(updatedStreamErrorRetries.get(0).source(), is(updatedStreamErrorRetry.source()));
        assertThat(updatedStreamErrorRetries.get(0).component(), is(updatedStreamErrorRetry.component()));
        assertThat(updatedStreamErrorRetries.get(0).occurredAt(), is(updatedStreamErrorRetry.occurredAt()));
        assertThat(updatedStreamErrorRetries.get(0).retryCount(), is(updatedStreamErrorRetry.retryCount()));
        assertThat(updatedStreamErrorRetries.get(0).nextRetryTime(), is(updatedStreamErrorRetry.nextRetryTime()));
    }

    @Test
    public void shouldFindByStreamIdSourceAndComponent() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(CONTEXT_NAME);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamErrorRetryRepository.findAll(), is(emptyList()));

        final StreamErrorRetry streamErrorRetry_1 = new StreamErrorRetry(
                randomUUID(),
                "some-source_1",
                "some-component_1",
                new UtcClock().now(),
                1L,
                new UtcClock().now().plusMinutes(20)
        );

        final StreamErrorRetry streamErrorRetry_2 = new StreamErrorRetry(
                randomUUID(),
                "some-source_2",
                "some-component_2",
                new UtcClock().now(),
                2L,
                new UtcClock().now().plusMinutes(20)
        );
        final StreamErrorRetry streamErrorRetry_3 = new StreamErrorRetry(
                randomUUID(),
                "some-source_3",
                "some-component_3",
                new UtcClock().now(),
                3L,
                new UtcClock().now().plusMinutes(20)
        );

        streamErrorRetryRepository.upsert(streamErrorRetry_1);
        streamErrorRetryRepository.upsert(streamErrorRetry_2);
        streamErrorRetryRepository.upsert(streamErrorRetry_3);

        final Optional<StreamErrorRetry> streamErrorRetry = streamErrorRetryRepository.findBy(
                streamErrorRetry_2.streamId(),
                streamErrorRetry_2.source(),
                streamErrorRetry_2.component());

        assertThat(streamErrorRetry.isPresent(), is(true));
        assertThat(streamErrorRetry.get(), is(streamErrorRetry_2));
    }

    @Test
    public void shouldGetStreamRetryCount() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(CONTEXT_NAME);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamErrorRetryRepository.findAll(), is(emptyList()));

        final long retryCount = 1L;

        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                randomUUID(),
                "some-source",
                "some-component",
                new UtcClock().now(),
                retryCount,
                new UtcClock().now().plusMinutes(20)
        );

        streamErrorRetryRepository.upsert(streamErrorRetry);

        assertThat(streamErrorRetryRepository.getRetryCount(
                streamErrorRetry.streamId(),
                streamErrorRetry.source(),
                streamErrorRetry.component()), is(retryCount));
    }

    @Test
    public void shouldReturnRetryCountOfZeroIfStreamNotFoundInStreamErrorRetryTable() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(CONTEXT_NAME);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamErrorRetryRepository.findAll(), is(emptyList()));

        final UUID unknownStreamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        assertThat(streamErrorRetryRepository.getRetryCount(unknownStreamId, source, component), is(0L));
    }

    @Test
    public void shouldRemoveStreamErrorRetry() throws Exception {
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(CONTEXT_NAME);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamErrorRetryRepository.findAll(), is(emptyList()));

        final StreamErrorRetry streamErrorRetry_1 = new StreamErrorRetry(
                randomUUID(),
                "some-source_1",
                "some-component_1",
                new UtcClock().now(),
                1L,
                new UtcClock().now().plusMinutes(20)
        );

        final StreamErrorRetry streamErrorRetry_2 = new StreamErrorRetry(
                randomUUID(),
                "some-source_2",
                "some-component_2",
                new UtcClock().now(),
                2L,
                new UtcClock().now().plusMinutes(20)
        );
        final StreamErrorRetry streamErrorRetry_3 = new StreamErrorRetry(
                randomUUID(),
                "some-source_3",
                "some-component_3",
                new UtcClock().now(),
                3L,
                new UtcClock().now().plusMinutes(20)
        );

        streamErrorRetryRepository.upsert(streamErrorRetry_1);
        streamErrorRetryRepository.upsert(streamErrorRetry_2);
        streamErrorRetryRepository.upsert(streamErrorRetry_3);

        assertThat(streamErrorRetryRepository.findAll().size(), is(3));

        streamErrorRetryRepository.remove(streamErrorRetry_2.streamId(), streamErrorRetry_2.source(), streamErrorRetry_2.component());

        final List<StreamErrorRetry> streamErrorRetries = streamErrorRetryRepository.findAll();

        assertThat(streamErrorRetries.size(), is(2));
        assertThat(streamErrorRetries, hasItems(streamErrorRetry_1, streamErrorRetry_3));
    }
}