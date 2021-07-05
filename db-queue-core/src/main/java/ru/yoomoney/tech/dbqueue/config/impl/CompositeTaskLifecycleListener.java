package ru.yoomoney.tech.dbqueue.config.impl;

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Composite listener. It allows combining several listeners into one.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class CompositeTaskLifecycleListener implements TaskLifecycleListener {

    @Nonnull
    private final List<TaskLifecycleListener> listeners;

    /**
     * Constructor
     *
     * @param listeners task listeners
     */
    public CompositeTaskLifecycleListener(@Nonnull List<TaskLifecycleListener> listeners) {
        this.listeners = Objects.requireNonNull(listeners, "listeners must not be null");
    }

    @Override
    public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                       @Nonnull TaskRecord taskRecord, long pickTaskTime) {
        listeners.forEach(l -> l.picked(shardId, location, taskRecord, pickTaskTime));
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord) {
        listeners.forEach(l -> l.started(shardId, location, taskRecord));
    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord,
                         @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
        listeners.forEach(l -> l.executed(shardId, location, taskRecord, executionResult, processTaskTime));
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord) {
        listeners.forEach(l -> l.finished(shardId, location, taskRecord));
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord,
                        @Nullable Exception exc) {
        listeners.forEach(l -> l.crashed(shardId, location, taskRecord, exc));
    }

}
