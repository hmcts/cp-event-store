package uk.gov.justice.services.eventsourcing.eventpublishing.helpers;

import uk.gov.justice.services.jmx.api.command.SystemCommand;
import uk.gov.justice.services.jmx.command.SystemCommandHandlerProxy;
import uk.gov.justice.services.jmx.command.SystemCommandStore;

import java.util.List;

import jakarta.enterprise.inject.Default;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Default
public class DummySystemCommandStore implements SystemCommandStore {

    @Override
    public SystemCommandHandlerProxy findCommandProxy(final SystemCommand systemCommand) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(final List<SystemCommandHandlerProxy> systemCommandProxies) {
    }
}
