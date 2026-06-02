package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;

import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;

public class ExceptionDetailsRetriever {

    @Inject
    private StackTraceProvider stackTraceProvider;

    @Inject
    private FrameworkClassNameFilter frameworkClassNameFilter;

    public ExceptionDetails getExceptionDetailsFrom(final Throwable exception) {

        final List<StackTraceElement> stackTraceElements = new ArrayList<>();
        final String stackTrace = stackTraceProvider.getStackTraceFrom(exception);

        Throwable cause = exception;
        Throwable rootCause = null;
        while (cause != null) {
            final List<StackTraceElement> currentStackTraceElements = stream(cause.getStackTrace())
                    .filter(frameworkClassNameFilter::isFrameworkOrContextClass)
                    .toList();

            stackTraceElements.addAll(0, currentStackTraceElements);

            cause = cause.getCause();

            if (cause != null) {
                rootCause = cause;
            }
        }

        return new ExceptionDetails(
                exception,
                ofNullable(rootCause),
                stackTraceElements,
                stackTrace);
    }
}
