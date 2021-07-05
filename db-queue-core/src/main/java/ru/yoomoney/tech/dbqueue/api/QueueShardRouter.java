package ru.yoomoney.tech.dbqueue.api;

import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;

/**
 * Dispatcher for sharding support.
 *
 * It evaluates designated shard based on task parameters.
 *
 * @param <T> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public interface QueueShardRouter<T, R extends DatabaseAccessLayer> {
    /**
     * Get designated shard for task parameters
     *
     * @param enqueueParams Parameters with typed payload to enqueue the task
     * @return Shard where task will be processed on
     */
    QueueShard<R> resolveShard(EnqueueParams<T> enqueueParams);
}
