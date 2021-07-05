package ru.yoomoney.tech.dbqueue.api.impl;

import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Поставщик задач в шардированную базу данных
 *
 * @param <T> тип задачи
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class ShardingQueueProducer<T, R extends DatabaseAccessLayer> implements QueueProducer<T> {

    private final QueueShardRouter<T, R> queueShardRouter;
    private final TaskPayloadTransformer<T> payloadTransformer;
    private final QueueConfig queueConfig;

    public ShardingQueueProducer(@Nonnull QueueConfig queueConfig,
                                 @Nonnull TaskPayloadTransformer<T> payloadTransformer,
                                 @Nonnull QueueShardRouter<T, R> queueShardRouter) {
        this.queueShardRouter = Objects.requireNonNull(queueShardRouter);
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        QueueShard<R> queueShard = queueShardRouter.resolveShard(enqueueParams);
        EnqueueParams<String> rawEnqueueParams = new EnqueueParams<String>()
                .withPayload(payloadTransformer.fromObject(enqueueParams.getPayload()))
                .withExecutionDelay(enqueueParams.getExecutionDelay())
                .withExtData(enqueueParams.getExtData());
        Long enqueueId = queueShard.getDatabaseAccessLayer().transact(() ->
                queueShard.getDatabaseAccessLayer().getQueueDao().enqueue(queueConfig.getLocation(), rawEnqueueParams));
        return new EnqueueResult(queueShard.getShardId(), Objects.requireNonNull(enqueueId, "id of enqueued task must not be null"));
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
    }
}
