package uk.gov.justice.services.event.sourcing.subscription.error;

import java.util.Optional;

import jakarta.inject.Inject;

public class ExceptionHashGenerator {

    @Inject
    private HashFromStringGenerator hashFromStringGenerator;

    public String createHashStringFrom(
            final StackTraceElement stackTraceElement,
            final String exceptionClassName,
            final Optional<String> causeClassName) {

        final String className = stackTraceElement.getClassName();
        final String methodName = stackTraceElement.getMethodName();
        final int lineNumber = stackTraceElement.getLineNumber();

        final StringBuilder stringBuilder = new StringBuilder()
                .append(className).append("_")
                .append(methodName).append("_")
                .append(lineNumber).append("_")
                .append(exceptionClassName);

        causeClassName.ifPresent(causeClass -> stringBuilder.append("_").append(causeClass));

        final String string = stringBuilder.toString();
        return hashFromStringGenerator.createHashFrom(string);
    }
}
