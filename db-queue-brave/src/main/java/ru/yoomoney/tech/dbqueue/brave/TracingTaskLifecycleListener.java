package ru.yoomoney.tech.dbqueue.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContextOrSamplingFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Task lifecycle listener with brave tracing support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class TracingTaskLifecycleListener implements TaskLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(TracingTaskLifecycleListener.class);

    private static final ThreadLocal<SpanAndScope> threadLocalSpan = new ThreadLocal<>();

    @Nonnull
    private final Tracing tracing;
    @Nonnull
    private final B3SingleFormatSpanConverter spanConverter;
    private final String traceField;

    public TracingTaskLifecycleListener(@Nonnull Tracing tracing, String traceField) {
        this.tracing = tracing;
        this.spanConverter = new B3SingleFormatSpanConverter(tracing);
        this.traceField = traceField;
    }

    @Override
    public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
        String queueName = location.getQueueId().asString();
        String traceInfo = taskRecord.getExtData().get(traceField);
        Span taskSpan = spanConverter.deserializeTraceContext(traceInfo)
                .map(ctx -> tracing.tracer().nextSpan(TraceContextOrSamplingFlags.create(ctx)))
                .orElseGet(() -> {
                    log.info("unknown trace context, creating new");
                    return tracing.tracer().newTrace();
                });
        taskSpan.name("qreceive " + queueName)
                .tag("queue.name", queueName)
                .tag("queue.operation", "receive")
                .kind(Span.Kind.CONSUMER);

        threadLocalSpan.set(new SpanAndScope(taskSpan, tracing.tracer().withSpanInScope(taskSpan.start())));
    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                         @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
        SpanAndScope spanAndScope = threadLocalSpan.get();
        threadLocalSpan.remove();
        spanAndScope.spanInScope.close();
        spanAndScope.span.finish();
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                        @Nullable Exception exc) {
        threadLocalSpan.get().span.error(exc);
    }

    private static final class SpanAndScope {
        @Nonnull
        final Span span;
        @Nonnull
        final Tracer.SpanInScope spanInScope;

        SpanAndScope(@Nonnull Span span, @Nonnull Tracer.SpanInScope spanInScope) {
            this.span = Objects.requireNonNull(span);
            this.spanInScope = Objects.requireNonNull(spanInScope);
        }
    }
}
