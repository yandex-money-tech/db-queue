package ru.yoomoney.tech.dbqueue.brave;

import brave.Span;
import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.TraceContext;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class TracingTaskLifecycleListenerTest {

    @Nonnull
    private Tracing createTracing() {
        return Tracing.newBuilder().propagationFactory(
                ExtraFieldPropagation.newFactoryBuilder(
                        B3Propagation.newFactoryBuilder()
                                .injectFormat(Span.Kind.PRODUCER, B3Propagation.Format.SINGLE)
                                .injectFormat(Span.Kind.CONSUMER, B3Propagation.Format.SINGLE)
                                .build()).addField("extra-field").build())
                .build();
    }

    @Test
    public void should_start_span() {
        Tracing tracing = createTracing();
        QueueShardId shardId = new QueueShardId("s1");
        QueueLocation location = QueueLocation.builder().withTableName("table").withQueueId(new QueueId("testqueue")).build();
        Map<String, String> extData = new LinkedHashMap<>();
        extData.put("trace_info", "b3=0000000000000001-0000000000000002-1");
        TaskRecord taskRecord = TaskRecord.builder()
                .withId(1).withReenqueueAttemptsCount(2).withTotalAttemptsCount(2)
                .withExtData(extData).build();

        TracingTaskLifecycleListener taskListener = new TracingTaskLifecycleListener(tracing, "trace_info");

        taskListener.started(shardId, location, taskRecord);

        TraceContext traceContext = tracing.tracer().currentSpan().context();
        assertThat(traceContext.traceId(), equalTo(1L));
        assertThat(traceContext.parentId(), equalTo(2L));
        assertThat(traceContext.sampled(), equalTo(true));
        taskListener.finished(shardId, location, taskRecord);
    }

    @Test
    public void should_start_valid_root_span() {
        Tracing tracing = createTracing();
        QueueShardId shardId = new QueueShardId("s1");
        QueueLocation location = QueueLocation.builder().withTableName("table").withQueueId(new QueueId("testqueue")).build();
        Map<String, String> extData = new LinkedHashMap<>();
        extData.put("trace_info", "invalid");
        TaskRecord taskRecord = TaskRecord.builder()
                .withId(1).withReenqueueAttemptsCount(2).withTotalAttemptsCount(2)
                .withExtData(extData).build();

        TracingTaskLifecycleListener taskListener = new TracingTaskLifecycleListener(tracing, "trace_info");

        taskListener.started(shardId, location, taskRecord);

        TraceContext traceContext = tracing.tracer().currentSpan().context();
        assertThat(traceContext.traceId(), notNullValue());
        assertThat(traceContext.parentIdString(), equalTo(null));
        taskListener.finished(shardId, location, taskRecord);
    }
}