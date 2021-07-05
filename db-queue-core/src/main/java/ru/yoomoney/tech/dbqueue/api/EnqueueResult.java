package ru.yoomoney.tech.dbqueue.api;

import ru.yoomoney.tech.dbqueue.config.QueueShardId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Task enqueue result
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class EnqueueResult {
    @Nonnull
    private final QueueShardId shardId;
    @Nonnull
    private final Long enqueueId;

    /**
     * Constructor
     *
     * @param shardId shard id
     * @param enqueueId sequence id
     */
    public EnqueueResult(@Nonnull QueueShardId shardId, @Nonnull Long enqueueId) {
        this.shardId = Objects.requireNonNull(shardId);
        this.enqueueId = Objects.requireNonNull(enqueueId);
    }

    /**
     * Shard identifier of added task
     *
     * @return shard id
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Identifier (sequence id) of added task
     *
     * @return sequence id
     */
    @Nonnull
    public Long getEnqueueId() {
        return enqueueId;
    }

    @Override
    public String toString() {
        return "EnqueueResult{" +
                "shardId=" + shardId +
                ", enqueueId=" + enqueueId +
                '}';
    }
}
