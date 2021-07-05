package ru.yoomoney.tech.dbqueue.api.impl;

import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Shard router without sharding. Might be helpful if you have single database.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class SingleQueueShardRouter<T, R extends DatabaseAccessLayer> implements QueueShardRouter<T, R> {

    @Nonnull
    private final QueueShard<R> queueShard;

    /**
     * Constructor
     *
     * @param queueShard
     */
    public SingleQueueShardRouter(@Nonnull QueueShard<R> queueShard) {
        this.queueShard = Objects.requireNonNull(queueShard, "queueShard must not be null");
    }

    @Override
    public QueueShard<R> resolveShard(EnqueueParams<T> enqueueParams) {
        return queueShard;
    }
}
