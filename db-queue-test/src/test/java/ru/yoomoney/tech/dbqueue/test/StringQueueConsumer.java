package ru.yoomoney.tech.dbqueue.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Queue consumer without payload transformation
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class StringQueueConsumer implements QueueConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(StringQueueConsumer.class);

    @Nonnull
    private final QueueConfig queueConfig;
    @Nonnull
    private final AtomicBoolean isTaskConsumed;

    public StringQueueConsumer(@Nonnull QueueConfig queueConfig,
                               @Nonnull AtomicBoolean isTaskConsumed) {
        this.queueConfig = requireNonNull(queueConfig);
        this.isTaskConsumed = requireNonNull(isTaskConsumed);
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        log.info("payload={}", task.getPayloadOrThrow());
        isTaskConsumed.set(true);
        return TaskExecutionResult.finish();
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }

}
