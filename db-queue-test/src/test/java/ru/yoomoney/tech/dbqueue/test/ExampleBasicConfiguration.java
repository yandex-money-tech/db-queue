package ru.yoomoney.tech.dbqueue.test;

import org.junit.Assert;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.MonitoringQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonList;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
public class ExampleBasicConfiguration {

    @Test
    public void example_config() throws InterruptedException {
        AtomicBoolean isTaskConsumed = new AtomicBoolean(false);
        DefaultDatabaseInitializer.createTable(DefaultDatabaseInitializer.PG_DEFAULT_TABLE_DDL, "example_task_table");

        SpringDatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                DatabaseDialect.POSTGRESQL, QueueTableSchema.builder().build(),
                DefaultDatabaseInitializer.getJdbcTemplate(),
                DefaultDatabaseInitializer.getTransactionTemplate());
        QueueShard<SpringDatabaseAccessLayer> shard = new QueueShard<>(new QueueShardId("main"), databaseAccessLayer);

        QueueId queueId = new QueueId("example_queue");
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("example_task_table")
                .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofMillis(100))
                        .build());


        ShardingQueueProducer<String, SpringDatabaseAccessLayer> shardingQueueProducer = new ShardingQueueProducer<>(
                config, NoopPayloadTransformer.getInstance(), new SingleQueueShardRouter<>(shard));
        QueueProducer<String> producer = new MonitoringQueueProducer<>(shardingQueueProducer, queueId);
        StringQueueConsumer consumer = new StringQueueConsumer(config, isTaskConsumed);

        QueueService queueService = new QueueService(singletonList(shard),
                new CompositeThreadLifecycleListener(singletonList(
                        new LoggingThreadLifecycleListener())),
                new CompositeTaskLifecycleListener(singletonList(
                        new LoggingTaskLifecycleListener())));
        queueService.registerQueue(consumer);
        queueService.start();
        producer.enqueue(EnqueueParams.create("example task"));
        sleep(1000);
        queueService.shutdown();
        queueService.awaitTermination(Duration.ofSeconds(10));
        Assert.assertTrue(isTaskConsumed.get());
    }

}
