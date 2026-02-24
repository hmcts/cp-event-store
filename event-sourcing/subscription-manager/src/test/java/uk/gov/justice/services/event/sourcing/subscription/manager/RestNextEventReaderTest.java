package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RestNextEventReaderTest {

    @InjectMocks
    private RestNextEventReader restNextEventReader;

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhenReadIsCalled() {

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> restNextEventReader.read(randomUUID(), 1L));

        assertThat(exception.getMessage(), is("RestNextEventReader is not yet implemented"));
    }
}
