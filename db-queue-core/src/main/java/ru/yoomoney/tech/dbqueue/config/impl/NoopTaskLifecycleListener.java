package ru.yoomoney.tech.dbqueue.config.impl;

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Empty listener for task processing lifecycle.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public final class NoopTaskLifecycleListener implements TaskLifecycleListener {

    private static final NoopTaskLifecycleListener INSTANCE = new NoopTaskLifecycleListener();

    @Nonnull
    public static NoopTaskLifecycleListener getInstance() {
        return INSTANCE;
    }

    @Override
    public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                       @Nonnull TaskRecord taskRecord, long pickTaskTime) {

    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult, long processTaskTime) {

    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord, @Nullable Exception exc) {

    }
}
