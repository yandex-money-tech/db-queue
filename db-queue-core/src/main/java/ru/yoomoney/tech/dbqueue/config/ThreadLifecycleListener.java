package ru.yoomoney.tech.dbqueue.config;

import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
public interface ThreadLifecycleListener {

    /**
     * Start of the task processing in the queue.
     * <p>
     * Always called.
     * <p>
     * Might be useful for setting values in the logging context or change thread name.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     */
    void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location);

    /**
     * Thread was executed and finished processing.
     * <p>
     * Called when normal end of task processing.
     * <p>
     * Might be useful for measuring performance of the queue.
     *
     * @param shardId        Shard identifier, which processes the queue.
     * @param location       Queue location.
     * @param taskProcessed  Attribute that task was taken and processed, no tasks for processing otherwise.
     * @param threadBusyTime Time in millis of the thread was running active before sleep.
     */
    void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime);

    /**
     * End of the task processing lifecycle and start of the new one.
     * <p>
     * Always called, even after {@link #crashed}.
     * <p>
     * Might be useful for logging context return or move the thread to the initial state.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     */
    void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location);

    /**
     * Queue failed with fatal error.
     * <p>
     * Client code cannot trigger that method call,
     * this method is called when task picking crashed.
     * <p>
     * Might be useful for logging and monitoring.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     * @param exc      An error caused the crash.
     */
    void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nullable Throwable exc);
}
