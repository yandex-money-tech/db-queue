package ru.yoomoney.tech.dbqueue.api.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Wrapper for queue producer with logging and monitoring support
 *
 * @param <T> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class MonitoringQueueProducer<T> implements QueueProducer<T> {

    private static final Logger log = LoggerFactory.getLogger(MonitoringQueueProducer.class);

    private final QueueProducer<T> queueProducer;
    private final QueueId queueId;
    private final BiConsumer<EnqueueResult, Long> monitoringCallback;

    /**
     * Constructor
     *
     * @param queueProducer
     * @param queueId
     * @param monitoringCallback
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<T> queueProducer,
                                   @Nonnull QueueId queueId,
                                   BiConsumer<EnqueueResult, Long> monitoringCallback) {
        this.queueProducer = Objects.requireNonNull(queueProducer);
        this.queueId = Objects.requireNonNull(queueId);
        this.monitoringCallback = monitoringCallback;
    }

    /**
     * Constructor
     *
     * @param queueProducer
     * @param queueId
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<T> queueProducer,
                                   @Nonnull QueueId queueId) {
        this(queueProducer, queueId, ((enqueueResult, id) -> {
        }));
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        log.info("enqueuing task: queue={}, delay={}", queueId, enqueueParams.getExecutionDelay());
        long startTime = System.currentTimeMillis();
        EnqueueResult enqueueResult = queueProducer.enqueue(enqueueParams);
        log.info("task enqueued: id={}, queueShardId={}", enqueueResult.getEnqueueId(), enqueueResult.getShardId());
        long elapsedTime = System.currentTimeMillis() - startTime;
        monitoringCallback.accept(enqueueResult, elapsedTime);
        return enqueueResult;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<T> getPayloadTransformer() {
        return queueProducer.getPayloadTransformer();
    }
}
