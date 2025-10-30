package uk.gov.justice.services.event.source.subscriptions.interceptors.it;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.core.interceptor.InterceptorContext.interceptorContextWithInput;
import static uk.gov.justice.services.core.postgres.OpenEjbConfigurationBuilder.createOpenEjbConfigurationBuilder;
import static uk.gov.justice.services.messaging.Envelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.schema.service.CatalogProducer;
import uk.gov.justice.schema.service.SchemaCatalogResolverProducer;
import uk.gov.justice.schema.service.SchemaCatalogService;
import uk.gov.justice.services.cdi.InitialContextProducer;
import uk.gov.justice.services.cdi.LoggerProducer;
import uk.gov.justice.services.common.annotation.ComponentNameExtractor;
import uk.gov.justice.services.common.configuration.GlobalValueProducer;
import uk.gov.justice.services.common.configuration.JndiBasedServiceContextNameProvider;
import uk.gov.justice.services.common.configuration.ValueProducer;
import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.core.accesscontrol.AccessControlFailureMessageGenerator;
import uk.gov.justice.services.core.accesscontrol.AllowAllPolicyEvaluator;
import uk.gov.justice.services.core.accesscontrol.DefaultAccessControlService;
import uk.gov.justice.services.core.accesscontrol.PolicyEvaluator;
import uk.gov.justice.services.core.annotation.Adapter;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.dispatcher.DispatcherCache;
import uk.gov.justice.services.core.dispatcher.DispatcherConfiguration;
import uk.gov.justice.services.core.dispatcher.DispatcherFactory;
import uk.gov.justice.services.core.dispatcher.EmptySystemUserProvider;
import uk.gov.justice.services.core.dispatcher.EnvelopePayloadTypeConverter;
import uk.gov.justice.services.core.dispatcher.JsonEnvelopeRepacker;
import uk.gov.justice.services.core.dispatcher.ServiceComponentObserver;
import uk.gov.justice.services.core.dispatcher.SystemUserUtil;
import uk.gov.justice.services.core.envelope.EnvelopeInspector;
import uk.gov.justice.services.core.envelope.EnvelopeValidationExceptionHandlerProducer;
import uk.gov.justice.services.core.envelope.EnvelopeValidator;
import uk.gov.justice.services.core.envelope.MediaTypeProvider;
import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.core.extension.BeanInstantiater;
import uk.gov.justice.services.core.extension.ServiceComponentScanner;
import uk.gov.justice.services.core.featurecontrol.FeatureControlAnnotationFinder;
import uk.gov.justice.services.core.interceptor.InterceptorCache;
import uk.gov.justice.services.core.interceptor.InterceptorChainEntry;
import uk.gov.justice.services.core.interceptor.InterceptorChainEntryProvider;
import uk.gov.justice.services.core.interceptor.InterceptorChainObserver;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.json.FileBasedJsonSchemaValidator;
import uk.gov.justice.services.core.json.JsonSchemaLoader;
import uk.gov.justice.services.core.json.PayloadExtractor;
import uk.gov.justice.services.core.json.SchemaCatalogAwareJsonSchemaValidator;
import uk.gov.justice.services.core.json.SchemaValidationErrorMessageGenerator;
import uk.gov.justice.services.core.mapping.ActionNameToMediaTypesMappingObserver;
import uk.gov.justice.services.core.mapping.DefaultMediaTypesMappingCache;
import uk.gov.justice.services.core.mapping.DefaultNameToMediaTypeConverter;
import uk.gov.justice.services.core.mapping.DefaultSchemaIdMappingCache;
import uk.gov.justice.services.core.mapping.MediaTypesMappingCacheInitialiser;
import uk.gov.justice.services.core.mapping.SchemaIdMappingCacheInitialiser;
import uk.gov.justice.services.core.mapping.SchemaIdMappingObserver;
import uk.gov.justice.services.core.producers.EnvelopeValidatorFactory;
import uk.gov.justice.services.core.producers.RequestResponseEnvelopeValidatorFactory;
import uk.gov.justice.services.core.producers.RequesterProducer;
import uk.gov.justice.services.core.producers.SenderProducer;
import uk.gov.justice.services.event.source.subscriptions.interceptors.SubscriptionEventInterceptor;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.JndiAppNameProvider;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreDataSourceNameProvider;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.messaging.DefaultJsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.jms.DefaultEnvelopeConverter;
import uk.gov.justice.services.messaging.jms.JmsMessagingConfiguration;
import uk.gov.justice.services.messaging.jms.OversizeMessageGuard;
import uk.gov.justice.services.messaging.logging.DefaultTraceLogger;
import uk.gov.justice.services.subscription.EventRangeNormalizer;
import uk.gov.justice.services.subscription.MissingEventRangeFinder;
import uk.gov.justice.services.subscription.MissingEventRangeStringifier;
import uk.gov.justice.services.subscription.ProcessedEventStreamSpliteratorFactory;
import uk.gov.justice.services.subscription.ProcessedEventStreamer;
import uk.gov.justice.services.subscription.ProcessedEventStreamerConfiguration;
import uk.gov.justice.services.subscription.ProcessedEventTrackingRepository;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;
import uk.gov.justice.services.subscription.PublishedEventReadConfiguration;
import uk.gov.justice.services.subscription.RangeNormalizationCalculator;
import uk.gov.justice.services.subscription.SpliteratorStreamFactory;
import uk.gov.justice.services.test.utils.common.envelope.TestEnvelopeRecorder;
import uk.gov.justice.services.test.utils.common.validator.DummyJsonSchemaValidator;
import uk.gov.justice.services.test.utils.core.handler.registry.TestHandlerRegistryCacheProducer;
import uk.gov.justice.services.test.utils.messaging.jms.DummyJmsEnvelopeSender;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.openejb.jee.Application;
import org.apache.openejb.jee.WebApp;
import org.apache.openejb.junit5.RunWithApplicationComposer;
import org.apache.openejb.testing.Classes;
import org.apache.openejb.testing.Configuration;
import org.apache.openejb.testing.Module;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@RunWithApplicationComposer
@Adapter(EVENT_LISTENER)
public class SubscriptionEventInterceptorIT {

    private static final String SOURCE = "my-context";
    private static final String COMPONENT_NAME = "EVENT_LISTENER";
    private static final String EVENT_ABC = SOURCE + ".event-abc";
    private static final String CONTEXT_ROOT = "core-test";

    @Resource(name = "openejb/Resource/frameworkviewstore")
    private DataSource dataSource;

    @Inject
    private InterceptorChainProcessor interceptorChainProcessor;

    @Inject
    private AbcEventHandler abcEventHandler;

    @Inject
    private ProcessedEventTrackingService processedEventTrackingService;

    @BeforeEach
    public void setup() throws Exception {
        final InitialContext initialContext = new InitialContext();
        initialContext.bind("java:/DS.SubscriptionEventInterceptorIT", dataSource);
        new DatabaseCleaner().cleanViewStoreTables("framework", "processed_event");
    }

    @Configuration
    public Properties postgresqlConfiguration() {
        return createOpenEjbConfigurationBuilder()
                .addInitialContext()
                .addPostgresqlViewStore()
                .build();
    }

    @Module
    @Classes(cdi = true, value = {
            AbcEventHandler.class,
            ServiceComponentScanner.class,
            RequesterProducer.class,
            ServiceComponentObserver.class,

            InterceptorChainProcessorProducer.class,
            InterceptorCache.class,
            InterceptorChainObserver.class,
            EventListenerInterceptorChainProvider.class,
            SubscriptionEventInterceptor.class,

            SenderProducer.class,
            DummyJmsEnvelopeSender.class,
            DefaultEnvelopeConverter.class,

            StringToJsonObjectConverter.class,
            DefaultJsonObjectEnvelopeConverter.class,
            ObjectToJsonValueConverter.class,
            ObjectMapperProducer.class,
            Enveloper.class,

            AccessControlFailureMessageGenerator.class,
            AllowAllPolicyEvaluator.class,
            DefaultAccessControlService.class,
            DispatcherCache.class,
            DispatcherFactory.class,
            EnvelopePayloadTypeConverter.class,
            JsonEnvelopeRepacker.class,
            PolicyEvaluator.class,

            LoggerProducer.class,
            EmptySystemUserProvider.class,
            SystemUserUtil.class,
            BeanInstantiater.class,
            UtcClock.class,

            GlobalValueProducer.class,
            EnvelopeValidationExceptionHandlerProducer.class,
            FileBasedJsonSchemaValidator.class,
            JsonSchemaLoader.class,
            DefaultTraceLogger.class,
            JdbcResultSetStreamer.class,
            PreparedStatementWrapperFactory.class,
            ViewStoreJdbcDataSourceProvider.class,
            ViewStoreDataSourceNameProvider.class,

            SchemaCatalogAwareJsonSchemaValidator.class,
            PayloadExtractor.class,
            DefaultNameToMediaTypeConverter.class,
            DefaultSchemaIdMappingCache.class,
            SchemaIdMappingObserver.class,

            CatalogProducer.class,
            SchemaCatalogService.class,
            SchemaCatalogResolverProducer.class,

            DefaultMediaTypesMappingCache.class,
            ActionNameToMediaTypesMappingObserver.class,

            SenderProducer.class,
            MediaTypeProvider.class,
            EnvelopeValidator.class,
            EnvelopeInspector.class,
            RequesterProducer.class,
            DummyJsonSchemaValidator.class,

            MediaTypesMappingCacheInitialiser.class,
            SchemaIdMappingCacheInitialiser.class,

            ProcessedEventTrackingService.class,
            ProcessedEventTrackingRepository.class,
            ComponentNameExtractor.class,

            JndiAppNameProvider.class,
            InitialContextProducer.class,

            SchemaValidationErrorMessageGenerator.class,
            EventSourceNameCalculator.class,

            DispatcherConfiguration.class,
            FeatureControlAnnotationFinder.class,
            TestHandlerRegistryCacheProducer.class,

            MissingEventRangeFinder.class,
            EventRangeNormalizer.class,
            RangeNormalizationCalculator.class,
            PublishedEventReadConfiguration.class,

            RequestResponseEnvelopeValidatorFactory.class,
            EnvelopeValidatorFactory.class,

            ProcessedEventStreamer.class,
            SpliteratorStreamFactory.class,
            ProcessedEventStreamerConfiguration.class,
            ProcessedEventStreamSpliteratorFactory.class,

            MissingEventRangeStringifier.class,
            JmsMessagingConfiguration.class,
            ValueProducer.class,
            OversizeMessageGuard.class,
            JndiBasedServiceContextNameProvider.class
    })

    public WebApp war() {
        return new WebApp()
                .contextRoot(CONTEXT_ROOT)
                .addServlet("TestApp", Application.class.getName());
    }

    @Test
    public void shouldUpdateCurrentEventNumberAfterEventIsProcessed() {

        final UUID metadataId = randomUUID();
        final UUID streamId = randomUUID();

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(metadataId)
                        .withName(EVENT_ABC)
                        .withStreamId(streamId)
                        .withSource(SOURCE)
                        .withEventNumber(1L)
                        .withPreviousEventNumber(0L),
                getJsonBuilderFactory().createObjectBuilder().build());

        assertThat(processedEventTrackingService.getLatestProcessedEventNumber(SOURCE, COMPONENT_NAME), is(0L));

        interceptorChainProcessor.process(interceptorContextWithInput(envelope));

        assertThat(processedEventTrackingService.getLatestProcessedEventNumber(SOURCE, COMPONENT_NAME), is(1L));
    }

    @ServiceComponent(EVENT_LISTENER)
    @ApplicationScoped
    public static class AbcEventHandler extends TestEnvelopeRecorder {

        @Handles(EVENT_ABC)
        public void handle(JsonEnvelope envelope) {
            record(envelope);
        }

    }

    public static class EventListenerInterceptorChainProvider implements InterceptorChainEntryProvider {

        @Override
        public String component() {
            return EVENT_LISTENER;
        }

        @Override
        public List<InterceptorChainEntry> interceptorChainTypes() {
            final List<InterceptorChainEntry> interceptorChainTypes = new ArrayList<>();
            interceptorChainTypes.add(new InterceptorChainEntry(1, SubscriptionEventInterceptor.class));
            return interceptorChainTypes;
        }
    }
}
