package ru.yoomoney.tech.dbqueue.config;

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Listener for task processing lifecycle.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public interface TaskLifecycleListener {

    /**
     * Event of task picking from the queue.
     * <p>
     * Triggered when there is a task in the queue, which is ready for processing.
     * <p>
     * Might be useful for monitoring problems with database performance.
     *
     * @param shardId      Shard identifier, which processes the queue.
     * @param location     Queue location.
     * @param taskRecord   Raw task data.
     * @param pickTaskTime Time spent on picking the task from the queue in millis.
     */
    void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                long pickTaskTime);

    /**
     * The start event of task processing.
     * <p>
     * Always triggered when task was picked.
     * <p>
     * Might be useful for updating a logging context.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     */
    void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord);

    /**
     * Event for completion of client logic when task processing.
     * <p>
     * Always triggered when task processing has completed successfully.
     * <p>
     * Might be useful for monitoring successful execution of client logic.
     *
     * @param shardId         Shard identifier, which processes the queue.
     * @param location        Queue location.
     * @param taskRecord      Raw task data.
     * @param executionResult Result of task processing.
     * @param processTaskTime Time spent on task processing in millis, without the time for task picking from the queue.
     */
    void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                  @Nonnull TaskExecutionResult executionResult, long processTaskTime);

    /**
     * Event for completion the task execution in the queue.
     * <p>
     * Always triggered when task was picked up for processing.
     * Called even after {@link #crashed}.
     * <p>
     * Might be useful for recovery of initial logging context state.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     */
    void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord);


    /**
     * Event for abnormal queue processing.
     * <p>
     * Triggered when unexpected error occurs during task processing.
     * <p>
     * Might be useful for tracking and monitoring errors in the system.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     * @param exc        An error caused the crash.
     */
    void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                 @Nullable Exception exc);

}
