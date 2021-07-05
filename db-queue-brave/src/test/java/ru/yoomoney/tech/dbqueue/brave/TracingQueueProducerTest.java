package ru.yoomoney.tech.dbqueue.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.TraceContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class TracingQueueProducerTest {

    @Nonnull
    private Tracing createTracing() {
        return Tracing.newBuilder().propagationFactory(
                ExtraFieldPropagation.newFactoryBuilder(
                        B3Propagation.newFactoryBuilder()
                                .injectFormat(Span.Kind.PRODUCER, B3Propagation.Format.SINGLE)
                                .injectFormat(Span.Kind.CONSUMER, B3Propagation.Format.SINGLE).build()).
                        addField("extra-field").build())
                .build();
    }

    @Test
    public void should_save_existing_trace_info() {
        Tracing tracing = createTracing();
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(tracing.tracer().newTrace().start())) {
            B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);
            String previousTraceId = tracing.tracer().currentSpan().context().traceIdString();
            ExtraFieldPropagation.set("extra-field", "2");
            QueueProducer<String> producer = mock(QueueProducer.class);

            TracingQueueProducer<String> tracingProducer = new TracingQueueProducer<String>(producer,
                    new QueueId("shard1"), tracing, "trace_info");
            when(producer.enqueue(any())).thenReturn(new EnqueueResult(new QueueShardId("shard1"), 1L));
            tracingProducer.enqueue(EnqueueParams.create("empty"));
            ArgumentCaptor<EnqueueParams> enqueueParams = ArgumentCaptor.forClass(EnqueueParams.class);
            verify(producer).enqueue(enqueueParams.capture());

            Map<String, String> extData = enqueueParams.getValue().getExtData();
            TraceContext traceContext = spanConverter.deserializeTraceContext(extData.get("trace_info")).get();

            assertThat(traceContext.traceIdString(), equalTo(previousTraceId));
            assertThat(traceContext.spanId(), not(equalTo(0L)));
            assertThat(ExtraFieldPropagation.get(traceContext, "extra-field"), equalTo("2"));
        }
    }

    @Test
    public void should_enqueue_in_new_trace() {
        Tracing tracing = createTracing();
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(tracing.tracer().newTrace().start())) {
            B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

            String previousTraceId = tracing.tracer().currentSpan().context().traceIdString();

            QueueProducer<String> enqueuer = mock(QueueProducer.class);
            TracingQueueProducer<String> tracingProducer = new TracingQueueProducer<>(enqueuer,
                    new QueueId("shard1"), tracing, "trace_info");
            when(enqueuer.enqueue(any())).thenReturn(new EnqueueResult(new QueueShardId("1"), 1L));
            tracingProducer.enqueueInNewTrace(EnqueueParams.create("empty"));
            ArgumentCaptor<EnqueueParams> enqueueParams = ArgumentCaptor.forClass(EnqueueParams.class);
            verify(enqueuer).enqueue(enqueueParams.capture());

            Map<String, String> extData = enqueueParams.getValue().getExtData();
            TraceContext traceContext = spanConverter.deserializeTraceContext(
                    extData.get("trace_info")).get();

            assertThat(traceContext.traceIdString(), not(equalTo(previousTraceId)));
        }
    }

}