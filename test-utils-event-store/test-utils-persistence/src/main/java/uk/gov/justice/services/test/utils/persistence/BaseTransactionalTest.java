package uk.gov.justice.services.test.utils.persistence;

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for tests that require managed persistence/transactions via JPA.
 */
public abstract class BaseTransactionalTest {

    @Inject
    UserTransaction userTransaction;

    @BeforeEach
    public final void setup() throws Exception {
        userTransaction.begin();
        setUpBefore();
    }

    @AfterEach
    public final void tearDown() throws Exception {
        tearDownAfter();
        userTransaction.rollback();
    }

    /**
     * Implement this method if you require to do something before the test
     */
    protected void setUpBefore() {

    }

    /**
     * Implement this method if you require to do something after the test
     */
    protected void tearDownAfter() {

    }
}
