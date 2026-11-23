package com.dataextraction.cdc_consumer;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import com.dataextraction.buffer.FlushedBufferHandler;
import com.dataextraction.buffer.impl.LocalFileStoreFlushedBufferHandler;
import com.dataextraction.cdc_consumer.impl.BufferedFileWriteConsumer;
import com.dataextraction.cdc_consumer.process.EngineShutdownCoordinator;
import com.dataextraction.cdc_consumer.process.WalIdleTimeoutWatcher;
import com.dataextraction.cdc_consumer.format.ChangeFormatSupport;
import com.dataextraction.cdc_consumer.format.SnapshotDependencies;
import com.dataextraction.cdc_consumer.format.StartedSnapshot;
import com.dataextraction.cdc_consumer.format.StreamingDependencies;
import com.dataextraction.config.CatalogProperties;
import com.dataextraction.config.CdcReplicationProperties;
import com.dataextraction.config.DataExtractionProperties;
import com.dataextraction.config.DatabaseProperties;
import com.dataextraction.config.DebeziumProperties;
import com.dataextraction.config.DatapipelinesProperties;
import com.dataextraction.config.FileWriterProperties;
import com.dataextraction.config.JwtProperties;
import com.dataextraction.config.SchemaRegistryProperties;
import com.dataextraction.config.SinkProperties;
import com.dataextraction.config.SlotProperties;
import com.dataextraction.datapipelines.DatapipelinesHttpClient;
import com.dataextraction.datapipelines.ExtractJobReporter;
import com.dataextraction.datapipelines.JwtTokenProvider;
import com.dataextraction.kafka.SchemaAwareKafkaProducerFactory;
import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.observability.Observability;
import com.dataextraction.observability.ObservabilityFactory;
import com.dataextraction.pg.meta.PostgresConnectionFactory;
import com.dataextraction.pg.meta.PostgresLsn;
import com.dataextraction.pg.meta.PostgresMetaDataDB;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.notify.impl.FileWriteObserverDataPipelines;
import com.dataextraction.notify.impl.FileWriteObserverExtractJob;
import com.dataextraction.notify.impl.FileWriteObserverKafka;
import com.dataextraction.schemaregistry.CdcSchemaRegistrar;
import com.dataextraction.schemaregistry.SchemaRegistrySerdeProvider;
import com.dataextraction.sink.ExtractRecordSinkFactory;
import com.dataextraction.sink.impl.AvroExtractRecordSinkFactory;
import com.dataextraction.sink.impl.ConnectAvroSchemas;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;

import com.dataextraction.config.KafkaProperties;

@ShellComponent
public class CdcConsumerCommands {
    private static final Logger log = LoggerFactory.getLogger(CdcConsumerCommands.class);

    private final CatalogProperties catalogProperties;
    private final DatapipelinesProperties datapipelinesProperties;
    private final DatabaseProperties databaseProperties;
    private final FileWriterProperties fileWriterProperties;
    private final SlotProperties slotProperties;
    private final DataExtractionProperties dataExtractionProperties;
    private final SchemaRegistryProperties schemaRegistryProperties;
    private final SinkProperties sinkProperties;
    private final KafkaProperties kafkaProperties;
    private final DebeziumProperties debeziumProperties;
    private final CdcReplicationProperties cdcReplicationProperties;
    private final ReplicationConnectorConfig replicationConnectorConfig = new ReplicationConnectorConfig();

    public CdcConsumerCommands(
            CatalogProperties catalogProperties,
            DatapipelinesProperties datapipelinesProperties,
            DatabaseProperties databaseProperties,
            FileWriterProperties fileWriterProperties,
            SlotProperties slotProperties,
            DataExtractionProperties dataExtractionProperties,
            SchemaRegistryProperties schemaRegistryProperties,
            SinkProperties sinkProperties,
            KafkaProperties kafkaProperties,
            DebeziumProperties debeziumProperties,
            CdcReplicationProperties cdcReplicationProperties) {
        this.catalogProperties = catalogProperties;
        this.datapipelinesProperties = datapipelinesProperties;
        this.databaseProperties = databaseProperties;
        this.fileWriterProperties = fileWriterProperties;
        this.slotProperties = slotProperties;
        this.dataExtractionProperties = dataExtractionProperties;
        this.schemaRegistryProperties = schemaRegistryProperties;
        this.sinkProperties = sinkProperties;
        this.kafkaProperties = kafkaProperties;
        this.debeziumProperties = debeziumProperties;
        this.cdcReplicationProperties = cdcReplicationProperties;
    }

    public DatapipelinesProperties getDatapipelinesProperties() {
        return datapipelinesProperties;
    }

    public DatabaseProperties getDatabaseProperties() {
        return databaseProperties;
    }

    public FileWriterProperties getFileWriterProperties() {
        return fileWriterProperties;
    }

    public SlotProperties getSlotProperties() {
        return slotProperties;
    }

    public DataExtractionProperties getDataExtractionProperties() {
        return dataExtractionProperties;
    }

    public SchemaRegistryProperties getSchemaRegistryProperties() {
        return schemaRegistryProperties;
    }

    public SinkProperties getSinkProperties() {
        return sinkProperties;
    }

    public KafkaProperties getKafkaProperties() {
        return kafkaProperties;
    }

    private KafkaProducer<String, FileWriteFlushEvent> createKafkaProducer(CdcSchemaRegistrar cdcSchemaRegistrar) {
        KafkaProperties kafkaProperties = getKafkaProperties();
        SchemaRegistrySerdeProvider schemaRegistrySerdeProvider = getSchemaRegistryProperties().createSchemaRegistrySerdeProvider();
        SchemaAwareKafkaProducerFactory producerFactory = new SchemaAwareKafkaProducerFactory(schemaRegistrySerdeProvider, kafkaProperties);
        return producerFactory.createProducer(FileWriteFlushEvent.class);
    }

    private CdcSchemaRegistrar createCdcSchemaRegistrar(Observability observability) {
        return getSchemaRegistryProperties().createCdcSchemaRegistrar(observability);
    }

    private FileWrittenObserver createFileWriteObserverKafka(KafkaProducer<String, FileWriteFlushEvent> producer, Observability observability) {
        return FileWriteObserverKafka.builder()
            .withProducer(producer)
            .withPublishTopics(getKafkaProperties().getTopics().toArray(String[]::new))
            .withObservability(observability)
            .build();
    }

    private FileWrittenObserver createFileWriteObserverDataPipelines(Observability observability) {
        HttpClient httpClient = HttpClient.newHttpClient();
        JwtProperties jwtProperties = getDatapipelinesProperties().getJwt();
        JwtTokenProvider jwtTokenProvider = jwtProperties.buildJwtTokenProvider();
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return FileWriteObserverDataPipelines.builder()
            .withObservability(observability)
            .withDatapipelinesProperties(datapipelinesProperties)
            .withHttpClient(httpClient)
            .withJwtTokenProvider(jwtTokenProvider)
            .withObjectMapper(objectMapper)
            .build();
    }

    private ExtractRecordSinkFactory createSinkFactory() {
        return new AvroExtractRecordSinkFactory(new ConnectAvroSchemas(getDataExtractionProperties().getExtractNamespace()));
    }

    private FlushedBufferHandler createFlushedBufferHandler(Observability observability) {
        return new LocalFileStoreFlushedBufferHandler(
                Path.of(getFileWriterProperties().getBasePath()),
                getFileWriterProperties().getEnvironment(),
                getFileWriterProperties().getOutputFormat().extension(),
                observability);
    }

    @ShellMethod(key = "cdc-snapshot", value = "SnapShot CDC Consumer")
    public String cdcSnapshot() throws IOException, InterruptedException {
        log.info("CDC Snapshot Consumer");
        String jobId = UUID.randomUUID().toString();
        Observability observability = ObservabilityFactory.getInstance().observability();
        CdcSchemaRegistrar cdcSchemaRegistrar = createCdcSchemaRegistrar(observability);
        KafkaProducer<String, FileWriteFlushEvent> producer = createKafkaProducer(cdcSchemaRegistrar);
        FileWrittenObserver fileWriteObserverKafka = createFileWriteObserverKafka(producer, observability);
        FileWrittenObserver fileWriteObserverDataPipelines = createFileWriteObserverDataPipelines(observability);
        ExtractJobReporter extractJobReporter = ExtractJobReporter.createOptional(
                createDatapipelinesHttpClient(observability),
                jobId,
                "snapshot",
                catalogProperties.getName(),
                dataExtractionProperties.getExtractNamespace(),
                databaseProperties.getDbname(),
                databaseProperties.getInstanceName());
        if (extractJobReporter != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    extractJobReporter::finish, "extract-job-reporter-shutdown"));
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }, "kafka-producer-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            fileWriteObserverDataPipelines.flush();
        }, "file-write-observer-data-pipelines-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            fileWriteObserverKafka.flush();
        }, "file-write-observer-kafka-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(ObservabilityFactory.getInstance()::shutdown, "observability-shutdown"));

        PostgresConnectionFactory postgresConnectionFactory = getPostgresConnectionFactory();
        PostgresMetaDataDB postgresMetaDataDB =
                new PostgresMetaDataDB(postgresConnectionFactory, observability);
        String slotName = this.databaseProperties.getSlotName();
        if (slotName != null && !slotName.isBlank()) {
            try {
                postgresMetaDataDB.logReplicationSlotStatus(slotName);
            } catch (SQLException e) {
                log.error("Error logging replication slot status", e.getMessage());
                return "Error logging replication slot status: " + e.getMessage();
            }            
        }
        String offsetFile = databaseProperties.getOffsetStorageFileFilename();
        if (offsetFile != null && !offsetFile.isBlank()) {
            log.info("Debezium offset file: {}", offsetFile);
        }


        // bulk copy pasta

        if (offsetFile != null && !offsetFile.isBlank()) {
            log.info("Debezium snapshot offset file: {}", offsetFile);
        }

        int maxBufferSizeBytes = dataExtractionProperties.getMaxBufferSizeBytes();

        String extractType = "snapshot";
        ExtractRecordSinkFactory sinkFactory = createSinkFactory();
        FlushedBufferHandler flushedBufferHandler = createFlushedBufferHandler(observability);

        SnapshotDependencies.Builder snapshotDependencies = SnapshotDependencies.builder()
                .withMaxBufferSize(maxBufferSizeBytes)
                .withObservability(observability)
                .addFileWrittenObserver(fileWriteObserverKafka)
                .addFileWrittenObserver(fileWriteObserverDataPipelines);
        if (extractJobReporter != null) {
            snapshotDependencies.addFileWrittenObserver(new FileWriteObserverExtractJob(extractJobReporter, observability));
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        AtomicReference<EngineShutdownCoordinator> coordinatorRef = new AtomicReference<>();

        resetSnapshotOffsetFile();
        Properties connectorProps = createSnapshotConnectorProps();
        DebeziumEngine.CompletionCallback completion = (success, message, error) -> {
            if (!success && error != null && !(error instanceof StopEngineException)) {
                log.warn("Snapshot engine stopped with error: {}", message, error);
                if (extractJobReporter != null) {
                    extractJobReporter.markFailed();
                }
            } else {
                log.info("Snapshot engine stopped: {}", message);
            }
            EngineShutdownCoordinator coordinator = coordinatorRef.get();
            if (coordinator != null) {
                coordinator.shutdown();
            }
        };
        StartedSnapshot started = ChangeFormatSupport.from(debeziumProperties, schemaRegistryProperties)
                .startSnapshot(connectorProps, snapshotDependencies
                        .withRunGuid(jobId)
                        .withInstanceName(databaseProperties.getInstanceName())
                        .withExtractType(extractType)
                        .withSchemaRegistrar(cdcSchemaRegistrar)
                        .withFlushedBufferHandler(flushedBufferHandler)
                        .withSinkFactory(sinkFactory)
                        .build(), completion);

        EngineShutdownCoordinator coordinator =
                new EngineShutdownCoordinator(started.engine(), started.consumer(), executor, shutdownLatch);
        coordinatorRef.set(coordinator);
        coordinator.registerShutdownHook();

        if (extractJobReporter != null) {
            extractJobReporter.start();
        }
        executor.execute(started.engineTask());
        coordinator.awaitShutdown();
        if (extractJobReporter != null) {
            extractJobReporter.finish();
        }
        ObservabilityFactory.getInstance().close();
        return "SNAPSHOT COMPLETED";
    }

    @ShellMethod(key = "cdc-replicate", value = "Replicate CDC changes up to the current WAL position")
    public String cdcReplicate() throws IOException, InterruptedException {
        log.info("CDC replication");
        String jobId = UUID.randomUUID().toString();
        Observability observability = ObservabilityFactory.getInstance().observability();
        CdcSchemaRegistrar cdcSchemaRegistrar = createCdcSchemaRegistrar(observability);
        KafkaProducer<String, FileWriteFlushEvent> producer = createKafkaProducer(cdcSchemaRegistrar);
        FileWrittenObserver fileWriteObserverKafka = createFileWriteObserverKafka(producer, observability);
        FileWrittenObserver fileWriteObserverDataPipelines = createFileWriteObserverDataPipelines(observability);
        ExtractJobReporter extractJobReporter = ExtractJobReporter.createOptional(
                createDatapipelinesHttpClient(observability),
                jobId,
                "cdc",
                catalogProperties.getName(),
                dataExtractionProperties.getExtractNamespace(),
                databaseProperties.getDbname(),
                databaseProperties.getInstanceName());
        if (extractJobReporter != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    extractJobReporter::finish, "extract-job-reporter-shutdown"));
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }, "kafka-producer-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            fileWriteObserverDataPipelines.flush();
        }, "file-write-observer-data-pipelines-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            fileWriteObserverKafka.flush();
        }, "file-write-observer-kafka-shutdown"));
        Runtime.getRuntime().addShutdownHook(new Thread(ObservabilityFactory.getInstance()::shutdown, "observability-shutdown"));

        PostgresConnectionFactory postgresConnectionFactory = getPostgresConnectionFactory();
        PostgresMetaDataDB postgresMetaDataDB =
                new PostgresMetaDataDB(postgresConnectionFactory, observability);
        String slotName = this.databaseProperties.getSlotName();
        if (slotName != null && !slotName.isBlank()) {
            try {
                postgresMetaDataDB.logReplicationSlotStatus(slotName);
            } catch (SQLException e) {
                log.error("Error logging replication slot status: {}", e.getMessage());
                return "Error logging replication slot status: " + e.getMessage();
            }
        }
        String offsetFile = databaseProperties.getCdcOffsetStorageFileFilename();
        if (offsetFile != null && !offsetFile.isBlank()) {
            log.info("Debezium replication offset file (kept across runs): {}", offsetFile);
        }

        int maxBufferSizeBytes = dataExtractionProperties.getMaxBufferSizeBytes();
        String extractType = "cdc";
        ExtractRecordSinkFactory sinkFactory = createSinkFactory();
        FlushedBufferHandler flushedBufferHandler = createFlushedBufferHandler(observability);

        StreamingDependencies.Builder streamingDependencies = StreamingDependencies.builder()
                .withMaxBufferSize(maxBufferSizeBytes)
                .withObservability(observability)
                .addFileWrittenObserver(fileWriteObserverKafka)
                .addFileWrittenObserver(fileWriteObserverDataPipelines);
        if (extractJobReporter != null) {
            streamingDependencies.addFileWrittenObserver(new FileWriteObserverExtractJob(extractJobReporter, observability));
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        AtomicReference<EngineShutdownCoordinator> coordinatorRef = new AtomicReference<>();
        AtomicReference<WalIdleTimeoutWatcher> idleTimeoutWatcherRef = new AtomicReference<>();

        Properties connectorProps = replicationConnectorConfig.create(databaseProperties);
        ChangeFormatSupport formatSupport = ChangeFormatSupport.from(debeziumProperties, schemaRegistryProperties);
        BufferedFileWriteConsumer<?> consumer = formatSupport.streamingConsumer(streamingDependencies
                .withRunGuid(jobId)
                .withInstanceName(databaseProperties.getInstanceName())
                .withExtractType(extractType)
                .withSchemaRegistrar(cdcSchemaRegistrar)
                .withFlushedBufferHandler(flushedBufferHandler)
                .withSinkFactory(sinkFactory)
                .build());
        consumer.init();
        DebeziumEngine.CompletionCallback completion = (success, message, error) -> {
            if (!success && error != null && !(error instanceof StopEngineException)) {
                log.warn("Replication engine stopped with error: {}", message, error);
                if (extractJobReporter != null) {
                    extractJobReporter.markFailed();
                }
            } else {
                log.info("Replication engine stopped: {}", message);
            }
            WalIdleTimeoutWatcher watcher = idleTimeoutWatcherRef.get();
            if (watcher != null) {
                watcher.cancel();
            }
            EngineShutdownCoordinator coordinator = coordinatorRef.get();
            if (coordinator != null) {
                coordinator.shutdown();
            }
        };
        DebeziumEngine<?> engine = formatSupport.openEngine(connectorProps, consumer, completion);
        EngineShutdownCoordinator coordinator =
                new EngineShutdownCoordinator(engine, consumer, executor, shutdownLatch);
        coordinatorRef.set(coordinator);
        coordinator.registerShutdownHook();
        consumer.setEngineShutdownCoordinator(coordinator);

        WalIdleTimeoutWatcher idleTimeoutWatcher = WalIdleTimeoutWatcher.createOptional(
                Long.toString(cdcReplicationProperties.getWalIdleTimeoutSeconds()), coordinator);
        if (idleTimeoutWatcher != null) {
            idleTimeoutWatcherRef.set(idleTimeoutWatcher);
            consumer.setWalIdleTimeoutWatcher(idleTimeoutWatcher);
            idleTimeoutWatcher.start();
        }

        if (cdcReplicationProperties.isHighWaterMarkEnabled()) {
            try {
                PostgresLsn batchEndLsn = postgresMetaDataDB.getHighWaterMark().getHighWaterMark();
                log.info("Batch end LSN captured before engine start: {}", batchEndLsn);
                consumer.setHighWaterMark(batchEndLsn);
            } catch (SQLException e) {
                log.error("Error reading WAL high water mark: {}", e.getMessage());
                WalIdleTimeoutWatcher watcher = idleTimeoutWatcherRef.get();
                if (watcher != null) {
                    watcher.cancel();
                }
                coordinator.shutdown();
                return "Error reading WAL high water mark: " + e.getMessage();
            }
        } else {
            log.info("High water mark stopping disabled; engine will run until WAL idle timeout or shutdown");
        }

        if (extractJobReporter != null) {
            extractJobReporter.start();
        }
        executor.execute(engine);
        coordinator.awaitShutdown();
        if (extractJobReporter != null) {
            extractJobReporter.finish();
        }
        ObservabilityFactory.getInstance().close();
        return "CDC REPLICATION COMPLETED";
    }

    private DatapipelinesHttpClient createDatapipelinesHttpClient(Observability observability) {
        return datapipelinesProperties.createDatePipelinesHttpClient(observability);
    }

    /**
     * A completed offset makes {@code initial_only} skip the snapshot and block in streaming.
     * Each snapshot run starts from an empty offset file, matching pgoutput_to_json.
     */
    private void resetSnapshotOffsetFile() throws IOException {
        String offsetFile = databaseProperties.getOffsetStorageFileFilename();
        if (offsetFile == null || offsetFile.isBlank()) {
            return;
        }
        Path offsetPath = Path.of(offsetFile);
        if (Files.deleteIfExists(offsetPath)) {
            log.info("Deleted snapshot offset file for fresh extract: {}", offsetPath.toAbsolutePath());
        }
    }

    private Properties createSnapshotConnectorProps() {
        Properties connectorProps = new Properties();
        connectorProps.setProperty("name", "snapshot-engine");
        connectorProps.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        connectorProps.setProperty("plugin.name", "pgoutput");
        connectorProps.setProperty("database.hostname", databaseProperties.getHostname());
        connectorProps.setProperty("database.port", Integer.toString(databaseProperties.getPort()));
        connectorProps.setProperty("database.user", databaseProperties.getUser());
        connectorProps.setProperty("database.password", databaseProperties.getPassword());
        connectorProps.setProperty("database.dbname", databaseProperties.getDbname());
        connectorProps.setProperty("slot.name", databaseProperties.getSlotName());
        connectorProps.setProperty("publication.name", databaseProperties.getPublicationName());
        connectorProps.setProperty("slot.drop.on.stop", "false");
        connectorProps.setProperty("topic.prefix", "snapshot");
        connectorProps.setProperty("snapshot.mode", "initial_only");
        connectorProps.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        connectorProps.setProperty("offset.storage.file.filename", databaseProperties.getOffsetStorageFileFilename());
        connectorProps.setProperty("record.processing.threads", "1");
        connectorProps.setProperty("snapshot.max.threads", "1");
        String tableIncludeList = databaseProperties.getTableIncludeList();
        if (tableIncludeList != null && !tableIncludeList.isBlank()) {
            connectorProps.setProperty("table.include.list", tableIncludeList.trim());
            log.info("Snapshot table include list: {}", tableIncludeList.trim());
        }
        return connectorProps;
    }

    private PostgresConnectionFactory getPostgresConnectionFactory() {
        return new PostgresConnectionFactory(
            databaseProperties.jdbcUrl(),
            databaseProperties.getUser(),
            databaseProperties.getPassword(),
            databaseProperties.getInstanceName()
        );
    }
}
