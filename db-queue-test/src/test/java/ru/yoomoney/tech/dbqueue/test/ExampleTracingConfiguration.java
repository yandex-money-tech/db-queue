package ru.yoomoney.tech.dbqueue.test;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import org.junit.Assert;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.MonitoringQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.brave.TracingQueueProducer;
import ru.yoomoney.tech.dbqueue.brave.TracingTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.config.impl.CompositeTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.CompositeThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonList;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class ExampleTracingConfiguration {

    @Test
    public void tracing_config() throws InterruptedException {
        AtomicBoolean isTaskConsumed = new AtomicBoolean(false);
        DefaultDatabaseInitializer.createTable(DefaultDatabaseInitializer.PG_TRACING_TABLE_DDL, "tracing_task_table");
        SpringDatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                DatabaseDialect.POSTGRESQL, QueueTableSchema.builder()
                .withExtFields(Collections.singletonList("trace_info")).build(),
                DefaultDatabaseInitializer.getJdbcTemplate(),
                DefaultDatabaseInitializer.getTransactionTemplate());
        QueueShard<SpringDatabaseAccessLayer> shard = new QueueShard<>(new QueueShardId("main"), databaseAccessLayer);

        QueueId queueId = new QueueId("tracing_queue");
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("tracing_task_table")
                .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofMillis(100))
                        .build());

        Tracing tracing = Tracing.newBuilder().currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
                .addScopeDecorator(ThreadContextScopeDecorator.create())
                .build()).build();
        ShardingQueueProducer<String, SpringDatabaseAccessLayer> shardingQueueProducer = new ShardingQueueProducer<>(
                config, NoopPayloadTransformer.getInstance(), new SingleQueueShardRouter<>(shard));
        TracingQueueProducer<String> tracingQueueProducer = new TracingQueueProducer<>(shardingQueueProducer, queueId, tracing, "trace_info");
        QueueProducer<String> producer = new MonitoringQueueProducer<>(tracingQueueProducer, queueId);
        StringQueueConsumer consumer = new StringQueueConsumer(config, isTaskConsumed);

        QueueService queueService = new QueueService(singletonList(shard),
                new CompositeThreadLifecycleListener(singletonList(
                        new LoggingThreadLifecycleListener())),
                new CompositeTaskLifecycleListener(Arrays.asList(
                        new TracingTaskLifecycleListener(tracing, "trace_info"),
                        new LoggingTaskLifecycleListener())));
        queueService.registerQueue(consumer);
        queueService.start();
        Span span = tracing.tracer().newTrace();
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(span)) {
            producer.enqueue(EnqueueParams.create("tracing task"));
        }
        sleep(1000);
        queueService.shutdown();
        queueService.awaitTermination(Duration.ofSeconds(10));
        Assert.assertTrue(isTaskConsumed.get());
    }

}
