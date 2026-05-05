package uk.gov.justice.services.eventstore.management.verification.commands;

import static uk.gov.justice.services.eventstore.management.commands.VerifyCatchupCommand.VERIFY_CATCHUP;

import uk.gov.justice.services.eventstore.management.commands.VerifyCatchupCommand;
import uk.gov.justice.services.eventstore.management.verification.process.EventStoreVerification;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.command.HandlesSystemCommand;
import uk.gov.justice.services.jmx.logging.MdcLoggerInterceptor;

import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.interceptor.Interceptors;

public class EventStoreVerificationCommandHandler {

    @Inject
    private EventStoreVerification eventStoreVerification;

    @Interceptors(MdcLoggerInterceptor.class)
    @HandlesSystemCommand(VERIFY_CATCHUP)
    public void verifyCatchup(
            final VerifyCatchupCommand verifyCatchupCommand,
            final UUID commandId,
            @SuppressWarnings("unused")
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {
        eventStoreVerification.verifyEventStore(commandId, verifyCatchupCommand);
    }
}
