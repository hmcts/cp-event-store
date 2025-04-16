# Change Log
All notable changes to this project will be documented in this file, which follows the guidelines
on [Keep a CHANGELOG](http://keepachangelog.com/). This project adheres to
[Semantic Versioning](http://semver.org/).

### [Unreleased]

## [17.102.3] - 2025-04-16
### Changed
- Update framework to 17.102.2 for:
  - Extended RestPoller to include custom PollInterval implementation and introduced FibonacciPollWithStartAndMax class

## [17.102.2] - 2025-03-19
### Removed
- Removed error handling from BackwardsCompatibleSubscriptionManager

## [17.102.1] - 2025-03-17
### Changed
 - Update framework to 17.102.1 for:
   - Oversized messages are now logged as `WARN` rather than `ERROR`

## [17.102.0] - 2025-03-12
### Added
- Error handling for event streams:
  - New table `stream_error` in viewstore
  - Exceptions thrown during event processing now stored in stream_error table
  - New nullable column `stream_error_id` in stream status table with constraint on stream_error table
  - New nullable column `stream_error_position` in stream status table
  - Exception stacktraces are parsed to find entries into our code and stored in stream_error table
  - New Interceptor `EntityManagerFlushInterceptor` for EVENT_LISTENER component that will always flush the Hibernate EntityManager to commit viewstore changes to the database
  - New JNDI value `event.error.handling.enabled` with default value of `false` to enable/disable error handling for events
  - Added new `updated_at` column to `stream_status` table
  - The columns `stream_id`, `component_name` and `source` on the `stream_error` table are now unique when combined
  - Inserts into `stream_error` now `DO NOTHING` if a row with the same `stream_id`, `component_name` and `source` on the `stream`error` already exists
  - Inserts into `stream_error` are therefore idempotent
  - Split out the stream_error table into `stream_error` and `stream_error_hash`
  - Errors set into stream_status table are `upserts` to handle the case where no stream yet exists in stream_status
  - Only one error per stream is set into stream_error table
  - Streams are marked as errored in stream_status table using stream_id, source and component
  - `source` and `component` added to StreamError Object
  - Marking a stream as fixed in stream_status table now handles the case when no stream yet exists in stream_status
### Changed
- Bump version to 17.102.x
- Optimised SnapshotJdbcRepository queries to fetch only required data


## [17.101.5] - 2025-01-16
### Changed
- Update microservice-framework to 17.101.6
### Removed
- Removed OWASP cross-site scripting check on html rest parameters introduced in microservice-framework release 17.6.1

## [17.101.4] - 2025-01-09
### Added
- Add dependency for org.ow2.asm version 9.3 (through maven-common-bom)
### Changed
- Update microservice-framework to 17.101.5
- Update framework-libraries to 17.101.2
- Update maven-parent-pom to 17.101.0
- Update postgresql.driver.version to 42.3.2 (through maven-parent-pom)
- Update maven-common-bom to 17.101.1
### Security
- Update com.jayway.json-path to version 2.9.0 to fix **security vulnerability CWE-787**
  Detail: https://cwe.mitre.org/data/definitions/787.html (through maven-common-bom)
- Update commons.io to 2.18.0 to fix security vulnerability CVE-2024-47554
  Detail: https://nvd.nist.gov/vuln/detail/CVE-2024-47554 and https://cwe.mitre.org/data/definitions/400.html

## [17.101.3] - 2024-12-20
### Changed
- Bump microservice-framework to 17.101.4
- Optimised SnapshotJdbcRepository queries to fetch only required data
### Added
- Expose prometheus metrics through /internal/metrics/prometheus endpoint
- Provide timerRegistrar bean to register timer with metricsRegistry
- Save aggregate snapshots asynchronously in the background when we have a large amount of event on a single stream. Default it 50000. This is configurable via JNDI var snapshot.background.saving.threshold
- Add 'liquibase.analytics.enabled: false' to all liquibase.properties files to stop liquibase collecting anonymous analytics if we should ever upgrade to liquibase 4.30.0 or greater. Details can be found here: https://www.liquibase.com/blog/product-update-liquibase-now-collects-anonymous-usage-analytics

## [17.100.8] - 2024-12-14
### Fixed
- Fixed error in RegenerateAggregateSnapshotBean where a closed java Stream was reused

## [17.100.7] - 2024-11-27
### Changed
- Jmx MBean `SystemCommanderMBean` now only takes basic Java Objects to keep the JMX handling interoperable
### Removed
- Removed `JmxCommandParameters` and `CommandRunMode` from JMX SystemCommanderMBean call

## [17.100.6] - 2024-11-27
### Removed
- Removed @MXBean annotation from Jmx interface class to change from MXBean to MBean

## [17.100.4] - 2024-11-22
### Fixed
- Removed test classes erroneously included in framework-command-client.jar
### Changed
- Improved error messages printed whilst running framework-command-client.jar

### Added
- Add correct type descriptions to RebuildSnapshotCommand 

## [17.100.3] - 2024-11-14
### Added
- New column `buffered_at` on the stream_buffer tables to allow for monitoring of stuck stream_buffer events

## [17.100.1] - 2024-11-12
### Changed
- Jmx commands can now have and extra optional String `command-runtime-string` that can ba
  passed to JmxCommandHandlers via the JmxCommandHandling framework
- All JmxCommandHandlers must now have `commandName` String, `commandId` UUID and JmxCommandRuntimeParameters in their method signatures
- Split filestore `content` tables back into two tables of `metadata` and `content` to allow for backwards compatibility with liquibase
### Added
- New parameter 'commandRuntimeString' to JMX commands
- New Jmx command `RebuildSnapshotCommand` and handler that can force hydration and generation of an Aggregate snapshot_
### Fixed
- JdbcResultSetStreamer now correctly streams data using statement.setFetchSize(). The Default fetch size is 200. This can be overridden with JNDI prop jdbc.statement.fetchSize

## [17.6.11] - 2024-10-14
### Fixed
- Fixed spelling mistake in OversizeMessageGuard error message

## [17.6.10] - 2024-10-11
### Added
- New method 'payloadIsNull()' on DefaultJsonEnvelope, to check if the payload is `JsonValue.NULL` or `null`
### Fixed
- Fix where null payloads of JsonEnvelopes get converted to `JsonValue.NULL` and cause a ClassCastException
- All JsonEnvelopes that have null payloads will now:
  - return `JsonValue.NULL` if `getPayload()` is called
  - throw `IncompatibleJsonPayloadTypeException` if `getPayloadAsJsonObject()` is called
  - throw `IncompatibleJsonPayloadTypeException` if `getPayloadAsJsonArray()` is called
  - throw `IncompatibleJsonPayloadTypeException` if `getPayloadAsJsonString()` is called
  - throw `IncompatibleJsonPayloadTypeException` if `getPayloadAsJsonNumber()` is called

## [17.6.9] - 2024-10-08
### Fixed
- Fixed the percentage of times that HIGH, MEDIUM and LOW priority jobs are run

## [17.6.7] - 2024-10-07
### Fixed
- Fixed test library accidentally put on compile scope

## [17.6.6] - 2024-09-25
### Changed
- Update framework-libraries to 17.6.4 in order to
  - Improve the fetching of jobs by priority from the jobstore by retrying with a different priority if the first select returns no jobs

## [17.6.5] - 2024-09-18
### Changed
- Update framework libraries to 17.6.3 in order to:
  - Refactor of File Store to merge file store 'metadata' table into the 'content' table.
  - File Store now only contains one table
  
## [17.6.4] - 2024-09-17
### Changed
- Update framework libraries to 17.6.2 in order to:
  - Update jobstore to process tasks with higher priority first
  - Fix for Jackson single argument constructor issue inspired from  https://github.com/FasterXML/jackson-databind/issues/1498
- The catchup process can now whitelist event sources to catchup
- New Jndi value can be set to `ALLOW_ALL` to allow all
### Added
- New Jndi value `java:global/catchup.event.source.whitelist` for a comma separated list of whitelisted event-sources for catchup.
### Fixed
- Fetch of PublishedEvents during catchup now correctly uses MultipleDataSourcePublishedEventRepository

## [17.6.3] - 2024-07-12
### Changed
- All events pulled from the event queue by the message driven bean now
  check the size of the message, and will log an error if the number of bytes
  is greater than a new jndi value `messaging.jms.oversize.message.threshold.bytes`
- All rest http parameters in the generated rest endpoints are now encoded using owasp to
  protect against cross site scripting

## [17.6.2] - 2024-07-05
### Added
- Adds created_at column to snapshot table and populate this while saving new snapshot
- 
## Changed
## [17.6.1] - 2024-06-18
### Fixed
- Fix transaction problem in ReplayEventToEventListerCommands by moving transaction boundry one layer down 

## [17.6.0] - 2024-06-13
### Fixed
- All streams now only have one (current) snapshot stored in the database. All older snapshots are deleted (CPI-890)
### Changed
- Merged in release-17.x.x branch to keep master up to date
- Update framework to 17.6.0
### Added
- Add REPLAY_EVENT_TO_EVENT_LISTENER and REPLAY_EVENT_TO_EVENT_INDEXER system command handlers to replay single event

## [17.5.1] - 2024-06-12
### Added
- Add maven-sonar-plugin to pluginManagement (through maven-parent-pom)

## [17.5.0] - 2024-06-05
- Update framework to 17.5.0 for:
  - JmsMessageConsumerClientProvider now returns JmsMessageConsumerClient interface rather than the implemening class

## [17.4.8] - 2024-06-04
### Changed
- Add method to SystemCommanderMBean interface to invoke system command without supplying CommandRunMode (through micro-service-framework changes)

## [17.4.6] - 2024-06-03
### Changed
- Break dependency on framework-command-client in test-utils-jmx library (by micro-service-framework changes)

## [17.4.5] - 2024-05-29
### Added
- Add REPLAY_EVENT_TO_EVENT_LISTENER and REPLAY_EVENT_TO_EVENT_INDEXER system command handlers to replay single event
### Changed
- Invoke eventBufferProcessor in transaction while replaying REPLAY_EVENT_TO_EVENT_LISTENER/REPLAY_EVENT_TO_EVENT_INDEXER system commands

## [17.4.4] - 2024-05-15
### Changed
- Release microservice-framework changes for adding junit CloseableResource that manages closing jms resources

## [17.4.3] - 2024-05-13
### Added
- Release micro-service-framework changes for adding jms message clients for effective management of jms resources in integration tests

## [17.4.2] - 2024-02-08
### Changed
- Add jobstore retry migration liquibase script via framework-libraries

## [17.4.1] - 2023-12-12
### Changed
- Add retry mechanism to jobstore via framework-libraries

## [17.4.0] - 2023-12-08
### Changed
- Update framework to 17.4.0

## [17.3.1] - 2023-11-27
### Changed
- Update common-bom to 17.2.1

## [17.3.0] - 2023-11-09
### Added
- Added dependencies required by various contexts to the framework-libraries-bom
- Added unifiedsearch-core and unifiedsearch-client-generator dependencies to framework-bom

## [17.2.0] - 2023-11-03
### Changed
- Centralise all generic library dependencies and versions into maven-common-bom
- Update common-bom to 17.2.0
### Removed
- Removed dependency on apache-drools as it's not used by any of the framework code
### Security
- Update common-bom to fix various security vulnerabilities in org.json, plexus-codehaus, apache-tika and google-guava

## [17.1.3] - 2023-09-06
### Fixed
- Fixed IndexOutOfBoundsException in ProcessedEventStreamSpliterator during catchup

## [17.1.2] - 2023-08-30
### Removed
- Remove `clientId` from the header of all generated Message Driven Beans

## [17.1.1] - 2023-07-19
### Changed
- Update junit to 5, surefire and failsafe plugins

## [17.0.2] - 2023-06-15
### Fixed
- Fix Logging of missing event ranges to only log on debug
- Limit logging of MissingEventRanges logged to sensible maximum number.
### Added
- New JNDI value `catchup.max.number.of.missing.event.ranges.to.log`
### Security
- Update org.json to version 20230227 to fix **security vulnerability CVE-2022-45688**
  Detail: https://nvd.nist.gov/vuln/detail/CVE-2022-45688

## [17.0.2] - 2023-05-30
### Fixed
- Fix batch fetch of processed events to load batches of 'n' events
  into a Java List in memory

## [17.0.1] - 2023-05-10
### Changed
- Update framework-libraries to 17.0.1 in order to:
    - Remove unnecessary logging of 'skipping generation' message in pojo generator


## [17.0.0] - 2023-05-05
### Changed
- Update to Java 17
- Pojo generator fixed to handle null for additionalProperties
- Update common-bom to 17.0.0 in order to:
    - Add byte-buddy 1.12.22 as a replacement for cglib
    - Downgrade h2 to 1.4.196 as 2.x.x is too strict for our tests
- Update framework-libraries to 17.0.0 in order to:
    - Change 'additionalProperties' Map in generated pojos to HashMap to allow serialization
### Removed
- Remove illegal-access argument from surefire plugin from plugin management (through maven-parent-pom 17.0.0-M6)
- Remove illegal-access argument (not valid for java 17) from sure fire plugin


## [11.0.1] - 2023-02-01
### Removed
- Removed unnecessary indexes from event_log table
- Downgraded maven minimum version to 3.3.9 until the pipeline maven version is updated

## [11.0.0] - 2023-01-26
### Changed
- Updated to Java 11
- Bumped version to 11 to match new framework version
- Update to JEE 8
- Updated JdbcBasedEventRepository to insert eventIs into the pre_publish_queue table directly
- Provide constant for artemis healthcheck name
- Provide capability to override destination names while performing artemis healthcheck
- Upgrade framework to access messaging-jms dependency through framework-bom
- Updated slf4j/log4j bridge jar from slf4j-log4j12 to slf4j-reload4j
- MessageProducerClient and MessageConsumerClient are now idempotent when start is called
- Moved healthcheck database table checker into microservices-framework
- Update liquibase to 4.10.0

### Added
- New healthcheck module
- Healthchecks for viewstore, eventstore, jobstore and filestore
- Added new healthcheck for the system database.
- Added healthcheck modules into the event store bom
- Add artemis health check implementation which gets automatically registered by existing healthcheck mechanism

### Removed
- Removed log4j-over-slf4j as it is now replaced by slf4j-reload4j
- Removed all old hamcrest libraries
- Removed strict checking of liquibase.properties files
- Removed the trigger from the event_log table and all related classes and commands
- Removed dependency on liquibase jars from test-utils-persistence

### Security
- Update hibernate version to 5.4.24.Final
- Update jackson.databind version to 2.12.7.1
- Update jackson libraries to 2.12.7
- Update wildfly to version 26.1.2.Final
- Update artemis to version 2.20.0
- Update resteasy-client to version 4.7.7.Final

## [7.2.2] - 2020-11-18
### Changed
- Updated framework to version 7.2.2

## [7.2.1] - 2020-11-18
### Changed
- Updated framework to version 7.2.1

## [7.2.0] - 2020-11-16
### Added
- Added support for FeatureControl toggling by annotating service component
  handler methods with @FeatureControl
### Changed
- Moved timer bean utilities to framework-libraries
- Updated framework to version 7.2.0

## [7.1.4] - 2020-10-16
### Changed
- Updated framework-libraries to version 7.1.5
- Builders of generated pojos now have a `withValuesFrom(...)` method
  to allow the builder to be initialised with the values of another pojo instance

## [7.1.3] - 2020-10-15
### Changed
- Security updates to apache.tika, commons.beanutils, commons.guava and junit in common-bom
- Updated common-bom to 7.1.1

## [7.1.2] - 2020-10-09
### Changed
- The context name is now used for creating JMS destination name if the event
  is an administration event

## [7.1.1] - 2020-09-25
### Changed
- Updated framework-libraries to version 7.1.1

## [7.1.0] - 2020-09-23
### Changed
- Updated parent maven-framework-parent-pom to version 2.0.0
- Updated framework to version 7.1.0
- Moved to new Cloudsmith.io repository for hosting maven artifacts
- Updated encrypted properties in travis.yaml to point to cloudsmith


## [7.0.10] - 2020-09-10
### Changed
- Update framework-libraries to 7.0.11

## [7.0.9] - 2020-08-28
### Changed
- Changed the removal of the event_log trigger to be called by a
  ServletContextListener to fix call happening after database
  connections destroyed


## [7.0.8] - 2020-08-14
### Changed
- Updated framework to 7.0.10

## [7.0.7] - 2020-08-13
### Changed
- Updated framework to 7.0.9

## [7.0.6] - 2020-07-24
### Added
- indexes added to stream_id and position_in_stream in event_log table
### Changed
- DefaultEventStoreDataSourceProvider changed to a singleton,
  so the caching of the DataSource works properly
- Test util class DatabaseCleaner has an additional method
  'cleanEventStoreTables(...)' for truncating specified tables
  in the event-store
- Update framework to 7.0.8

## [7.0.5] - 2020-07-08
### Changed
- Update framework to 7.0.7
- Updated travis.yaml security setting for bintray to use the new cjs user

## [7.0.4] - 2020-06-03
### Changed
- Update framework to 7.0.6

## [7.0.3] - 2020-05-29
### Changed
- Update framework to 7.0.5

## [7.0.2] - 2020-05-27
### Changed
- Update framework to 7.0.4

## [7.0.1] - 2020-05-22
### Removed
- jboss-ejb3-ext-api from dependency-management

## [7.0.0] - 2020-05-22
### Changed
- Changed dependencies on only depend on framework 7.0.3
- Bumped version to 7.0.0 to match other framework libraries

## [2.4.13] - 2020-04-23
### Changed
- Update framework to 6.4.2

## [2.4.12] - 2020-04-23
### Failed Release
- Github issues

## [2.4.11] - 2020-04-14
### Added
- Added a test DataSource for the file-service database

## [2.4.10] - 2020-04-09
### Added
- Added indexes to processed_event table

### Changed
- microservice-framework -> 6.4.1

## [2.4.9] - 2020-03-04
### Changed
- Fail cleanly if exception occurs while accessing subscription event source

## [2.4.8] - 2020-01-29
### Changed
- Inserts into the event-buffer no longer fails if there is a conflict; it just logs a warning

## [2.4.7] - 2020-01-24
### Changed
- Event store now works with multiple event sources
- Event store now compatible with contexts that do not have a command pillar
- Extracted all command pillar SystemCommands into their own module

## [2.4.6] - 2020-01-21
### Added
- Catchup for multiple components now run in order of component and subscription priority
- Added event source name to catchup logger output
### Fixed
- Fixed catchup error where catchup was marked as complete after all subscriptions rather than all components

## [2.4.5] - 2020-01-06
### Removed
- Remove mechanism to also drop/add trigger on SUSPEND/UNSUSPEND as it causes
  many strange ejb database errors

## [2.4.4] - 2020-01-06
### Added
- Added mechanism to also drop/add trigger to event_log table on SUSPEND/UNSUSPEND commands
### Fixed
- Fixed potential problem of a transaction failing during catchup causing catchup to never complete

## [2.4.3] - 2019-12-06
### Changed
- Backpressure added to the event processing queues during catchup
### Fixed
- Verification completion log message now correctly logs if verification of Catchup or of Rebuild

## [2.4.2] - 2019-11-25
### Added
- Catchup Log message to show that all active events are now waiting to be consumed

## [2.4.1] - 2019-11-20
### Changed
- Now batch inserting PublishedEvents on rebuild to speed up the command
- Changed batch size on PublishedEvent rebuild to 1,000 per batch

## [2.4.0] - 2019-11-13
### Added
New SystemCommand VERIFY_REBUILD to verify the results of of the rebuild
- Verifies that the number of active events in event_log matches the number of events in published_event
- Verifies that each event_number in published_event correctly links to an existing previous_event
- Verifies that each active stream has at least one event
### Changed
- SHUTTER command renamed to SUSPEND
- UNSHUTTER command renamed to UNSUSPEND
- The database trigger for publishing on the event_log table is now added on application
  startup and removed on application shut down
- Updated to framework 6.4.0

## [2.3.1] - 2019-11-07
### Fixed
- removed rogue logging of payload during event validation

## [2.3.0] - 2019-11-07
### Added
- Added event_id to the processed_event table to aid debugging of publishing
### Changed
- Event-Store SystemCommands moved into this project to break the dependency on framework

## [2.2.7] - 2019-11-04
### Added
- New command 'ValidatePublishedEventsCommand' and handler for validating all events in
  event_log against their schemas
### Changed
- Updated framework to 6.2.5
- Updated Json Schema Catalog to 1.7.6

## [2.2.6] - 2019-10-30
### Changed
- Improved the event_log query to determine if the renumber of events is complete.
  Changed to use select MAX rather than count(*)

## [2.2.5] - 2019-10-29
### Changed
- Removed asynchronous bean to run catchup queue and replaced with ManagedExecutor

## [2.2.4] - 2019-10-28
### Fixed
- During catchup each event is now processed in a separate transaction

## [2.2.3] - 2019-10-25
### Fixed
- Catchup range processing

## [2.2.2] - 2019-10-24
### Changed
- Pre publish and publish timer beans now run in a separate thread.
- New JNDI boolean values of 'pre.publish.disable' and 'publish.disable' to disable
  the running of PrePublisherTimerBean and PublisherTimerBean respectively
- Error message of event linking verification now gives more accurate error messages
  based on whether the problem is in published_event or processed_event
- New SystemCommands EnablePublishingCommand and DisablePublishingCommand for enabling/disabling the publishing beans
- Catchup will check for all missing events in the processed_event table and Catchup only the missing event ranges

## [2.2.1] - 2019-10-18
### Fixed
- VERIFY_CATCHUP now correctly marks its status as COMMAND_FAILED if any of
  the verification steps fail. Verification warnings are considered successful

## [2.2.0] - 2019-10-15
### Added
- New table in System database 'system_command_status' for storing state of commands
### Changed
- Updated framework to 6.2.0
- All system commands now store their state in the system_command_status table in the system
  database. This is to allow the JMX client to wait until the command has completed or failed
  before it exits.
- Now using CatchupCommand to determine if we are running Event or Indexer catchup
- Converted ShutteringExecutors to use the new ShutteringExecutor interface
- Added commandId to all SystemEvents
- All SystemCommand handlers now take a mandatory UUID, commandId.
- Moved MdcLogger to framework 'jmx-command-handling' module

## [2.1.2] - 2019-10-01
### Changed
- Updated to framework 6.1.1
- All SystemCommand handlers now take a mandatory UUID, commandId.

## [2.1.1] - 2019-09-28
### Added
- New system event 'CatchupProcessingOfEventFailedEvent' fired if processing of any PublishedEvent during catchup fails
### Changed
- All system events moved into their own module 'event-store-management-events'
- Unsuccessful catchups now logged correctly in catchup completion.

## [2.1.0] - 2019-09-26
### Changed
- Subscriptions are no longer run asynchronously during catchup. Change required for MI catchup.
- Event catchup and Indexer catchup now run the same code

## [2.0.25] - 2019-10-08
### Fixed
- Fix single event rebuilding of published_event table

## [2.0.24] - 2019-10-07
### Fixed
- Fix issue where more than 1000 inactive events stops the rebuild process

### Changed
Run the renumbering of events in a batch

## [2.0.23] - 2019-10-04
### Changed
- Updated framework to 6.0.17

## [2.0.22] - 2019-09-24
### Changed
- Catchup verification logging now runs in MDC context

## [2.0.21] - 2019-09-23
### Added
- New SystemCommand to verify the results of running catchup
    - Verifies that the number of active events in event_log matches the number of events in published_event
    - Verifies that the number of events in published_event matches the number of events in processed_event
    - Verifies that the stream_buffer table is empty
    - Verifies that each event_number in published_event correctly links to an existing previous_event
    - Verifies that each event_number in processed_event correctly links to an existing previous_event
    - Verifies that each active stream has at least one event

## [2.0.20] - 2019-09-23
### Fixed
- Fixed name of publish queue trigger

## [2.0.19] - 2019-09-19
### Added
- New SystemCommands AddTrigger and RemoveTrigger to manage the trigger on the event_log table
- MDC logging of the service context for JMX commands

## [2.0.18] - 2019-09-18
### Changed
- Use DefaultEnvelopeProvider in MetadataEventNumberUpdater directly to fix classloading errors during rebuild
- Add logging to catchup processing, log every 1000th event
- Moved the conversion of PublishedEvent to JsonObject for the publishing of catchup inside the multi-threaded code

## [2.0.17] - 2019-09-16
### Changed
- Use DefaultJsonEnvelopeProvider in EventConverted directly to fix classloading errors during rebuild
- The event-source-name is now always calculated from the name of the event and never from its source field

## [2.0.16] - 2019-09-13
### Changed
- Renumbering of events during rebuild is now run in batches to allow shorter transactions

## [2.0.15] - 2019-09-11
### Changed
- Process rebuild in pages of events
- Changed transaction type of EventCatchupProcessorBean to NEVER to fix timeouts for long running transactions
- Reduced the maximum runtime for each iteration of the publishing beans to 450 milliseconds
- Long running transaction during rebuild broken into separate transactions
- Update framework to 6.0.14

## [2.0.14] - 2019-09-08
### Changed
- Update framework to 6.0.12

## [2.0.13] - 2019-09-06
### Changed
- Events are now renumbered according to their date_created order during rebuild

## [2.0.12] - 2019-09-04
### Changed
- If the event source name in the JsonEnvelope is missing is calculated from the event name rather
  than throwing an exception

## [2.0.11] - 2019-08-30
### Changed
- Update framework to 6.0.11

## [2.0.10] - 2019-08-28
### Changed
- Update framework to 6.0.10

## [2.0.9] - 2019-08-21
### Changed
- Update framework to 6.0.9

## [2.0.8] - 2019-08-21
### Changed
- Update framework to 6.0.8

## [2.0.7] - 2019-08-20
### Changed
- Update framework to 6.0.7

## [2.0.6] - 2019-08-19
### Changed
- Update framework to 6.0.6

## [2.0.5] - 2019-08-19
### Changed
- Update framework to 6.0.5

## [2.0.4] - 2019-08-16
### Changed
- Update framework to 6.0.4

## [2.0.3] - 2019-08-16
### Changed
- Update framework to 6.0.3

## [2.0.2] - 2019-08-15
### Changed
- Update framework to 6.0.2

## [2.0.1] - 2019-08-15
### Changed
- Update framework to 6.0.1

## [2.0.0] - 2019-08-15

### Added
- Event Catchup is now Observable using the JEE event system
- Observers for the shutter then catchup process
- Add Shuttering implementation to PublisherTimerBean
- Added an Observer for PublishedEvent Rebuild
- CommandHandlerQueueChecker to check the command handler queue is empty when shuttering
- Added implementation for updating StreamStatus table with component column
- Add Published Event Source
- Add event linked tracking for processed events
- Subscription prioritisation
- Catchup now callable from JMX bean
- Support to for linked events after stream transformation
- Java API to retrieve all stream IDs (active and inactive ones)
- truncate() method to the EventSource interface
- populate() method to the EventSource interface
- Added component to the event buffer to allow it to handle both the event listener and indexer
- Added component column to processed_event table to allow Event Listener and Indexer to create unique primary keys

### Changed
- Catchup and rebuild moved to a new 'event-store-management' module
- Catchup and rebuild now use the new System Command jmx architecture
- Shuttering executors no longer register themselves but are discovered at system startup and registered
- Implement usage of new system database
- Pre-publish and publish timer beans run for a given timer max runtime. The max time can be set with JNDI values "pre.publish.timer.max.runtime.milliseconds" and "event.dequer.timer.max.runtime.milliseconds".
- Renamed subscription-repository to event-tracking
- Add event-buffer-core and event-subscription-registry pom entries
- Moved shuttering from the publishing timer bean into the command api in framework
- Events for catchup and rebuild moved in from framework-api
- Merged event-publisher modules into one
- Event-Buffer functionality with changes to StreamBuffer & StreamStatus tables to support EventIndexer
- PrePublishBean now limits the maximum number of events published per run of the timer bean.
- No longer passing around event store data source JNDI name. Using default name instead
- Simplify datasource usage and setup
- Moved Subscription domain, parsing classes and builders to Framework
- Create concurrent catchup process, replays events on different streams concurrently
- Catchup now returns events from published_event rather than from event_log
- Replaced BackwardsCompatibleJsonSchemaValidator with DummyJsonSchemaValidator in Integration Tests
- Catchup, Rebuild and Index no longer call shutter/unshutter when running
- Improved utility classes for getting Postgres DataSources
- Renamed TestEventInserter to EventStoreDataAccess as an improved test class
- Added a findEventsByStreamId() method to EventStoreDataAccess
- Moved system commands to framework jmx-api
- Updated framework-api to 4.0.1
- Updated framework to 6.0.0
- Updated common-bom to 2.4.0
- Updated utilities to 1.20.1
- Updated test-utils to 1.24.3

## [1.1.3] - 2019-02-04
### Changed
- Update utilities to 1.16.4
- Update test-utils to 1.22.0
- Update framework to 5.1.1
- Update framework-domain to 1.1.1
- Update common-bom to 1.29.0
- Update framework-api to 3.2.0

## [1.1.2] - 2019-01-22
### Added
- JNDI configuration variable to enable/disable Event Catchup

## [1.1.1] - 2019-01-15
### Added
- Event Catchup on startup, where all unknown events are retrieved from the EventSource and played
- An event-number to each event to allow for event catchup
- Added a new TimerBean 'PrePublishBean'
- Added a new auto incrementing column event_number to event_log table
- Subscription event interceptor to update the current event number in the subscriptions table
- New util module
- Subscription repository
- Subscription liquibase for subscription table
- Better logging for event catchup
- A new transaction is started for each event during event catchup
- Event catchup only runs with event listener components
- Indexes to 'name' and 'date_created' columns in the event_log table
- New util module
- Better logging for event catchup
- An event-number to each event to allow for event catchup
- Added a new TimerBean 'PrePublishBean'
- Added a new auto incrementing column event_number to event_log table
- Subscription liquibase for subscription table
- Subscription repository
- Subscription event interceptor to update the current event number in the subscriptions table

### Changed
- Updated publish process to add events into a pre_publish_queue table
- Renamed the sequence_number column in event_stream to position_in_stream
- Renamed the sequence_id column in event_log to position_in_stream
- Tightened up transaction boundaries for event catchup so that each event is run in its own transaction
- Event catchup now delegated to a separate session bean to allow for running in RequestScope
- Current event number is initialised to zero if it does not exist on app startup
- Tightened up transaction boundaries for event catchup so that each event is run in its own transaction
- A new transaction is started for each event during event catchup
- Updated publish process to add events into a pre_publish_queue table
- Renamed the sequence_id column in event_log to position_in_stream
- Renamed the sequence_number column in event_stream to position_in_stream

## [1.0.4] - 2018-11-16
### Changed
- Updated framework-api to 3.0.1
- Updated framework to 5.0.4
- Updated framework-domain to 1.0.3

### Added
- Added a page size when reading stream of events

## [1.0.3] - 2018-11-13
### Changed
- Removed hard coded localhost from test datasource url

## [1.0.2] - 2018-11-09
### Changed
- Update framework to 5.0.3

## [1.0.1] - 2018-11-07
### Changed
- Update framework to 5.0.2

## [1.0.0] - 2018-11-07
### Changed
- Update framework to 5.0.0
- Renamed DatabaseCleaner method to match table name
- Removed the need to have event-source.yaml
- Remove requirement to have a subscription-descriptor.yaml on the classpath
- event-publisher-process to event-store-bom
- New test-utils-event-store module to hold TestEventRepository moved from framework
- Reverted names of event buffer tables to their original names:
  subscription -> stream_status, event-buffer -> stream buffer
- Moved test-utils-persistence into this project from
  [Framework](https://github.com/CJSCommonPlatform/microservice_framework)

### Added
- Extracted project from all event store related modules in Microservices Framework 5.0.0-M1: https://github.com/CJSCommonPlatform/microservice_framework


