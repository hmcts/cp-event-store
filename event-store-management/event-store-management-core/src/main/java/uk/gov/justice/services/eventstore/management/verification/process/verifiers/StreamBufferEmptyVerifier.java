package uk.gov.justice.services.eventstore.management.verification.process.verifiers;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.error;
import static uk.gov.justice.services.eventstore.management.verification.process.VerificationResult.success;

import uk.gov.justice.services.eventstore.management.verification.process.TableRowCounter;
import uk.gov.justice.services.eventstore.management.verification.process.VerificationResult;
import uk.gov.justice.services.eventstore.management.verification.process.Verifier;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.util.List;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

public class StreamBufferEmptyVerifier implements Verifier {

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private TableRowCounter tableRowCounter;

    @Inject
    private Logger logger;

    @Override
    public List<VerificationResult> verify() {

        logger.info("Verifying that the stream buffer does not contain any unprocessed events...");

        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        final int rowCount = tableRowCounter.countRowsIn("stream_buffer", viewStoreDataSource);

        if (rowCount == 0) {
            return singletonList(success("stream_buffer table is empty"));
        }

        return singletonList(error(format("%d events found in the stream_buffer table", rowCount)));
    }
}
