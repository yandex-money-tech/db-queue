package ru.yoomoney.tech.dbqueue.config.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thread listener with logging support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class LoggingThreadLifecycleListener implements ThreadLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingThreadLifecycleListener.class);

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime) {
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nullable Throwable exc) {
        log.error("fatal error in queue thread: shardId={}, location={}", shardId.asString(),
                location, exc);
    }
}
