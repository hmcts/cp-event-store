package uk.gov.justice.services.eventsourcing.util.jee.timer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import javax.ejb.Timer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SufficientTimeRemainingCalculatorFactoryTest {

    @InjectMocks
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @Test
    public void shouldCreateNewSufficientTimeRemainingCalculatorWithCorrectMembers() throws Exception {

        final Long timeBetweenRunsMillis = 10L;
        final Timer timer = mock(Timer.class);

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator =
                sufficientTimeRemainingCalculatorFactory.createNew(timer, timeBetweenRunsMillis);

        assertThat(getValueOfField(sufficientTimeRemainingCalculator, "timer", Timer.class), is(timer));
        assertThat(getValueOfField(sufficientTimeRemainingCalculator, "timeBetweenRunsMillis", Long.class), is(timeBetweenRunsMillis));
    }
}