package ru.yoomoney.tech.dbqueue.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.TraceContext;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class B3SingleFormatSpanConverterTest {

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
    public void should_handle_invalid_trace_info() {
        Tracing tracing = createTracing();
        B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

        Optional<TraceContext> context;
        context = spanConverter.deserializeTraceContext(null);
        assertThat(context.isPresent(), equalTo(false));
        context = spanConverter.deserializeTraceContext("");
        assertThat(context.isPresent(), equalTo(false));
        context = spanConverter.deserializeTraceContext("\n ");
        assertThat(context.isPresent(), equalTo(false));
        context = spanConverter.deserializeTraceContext("k=");
        assertThat(context.isPresent(), equalTo(false));
        context = spanConverter.deserializeTraceContext("k");
        assertThat(context.isPresent(), equalTo(false));
    }

    @Test
    public void should_extract_required_fields() {
        Tracing tracing = createTracing();
        B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

        String traceInfo = "b3=0000000000000001-0000000000000002-0\nextra-field=1";
        TraceContext context = spanConverter.deserializeTraceContext(traceInfo).get();
        Map<String, String> baggage = new HashMap<>();
        baggage.put("extra-field", "1");
        assertThat(context.traceId(), equalTo(1L));
        assertThat(context.spanId(), equalTo(2L));
        assertThat(context.sampled(), equalTo(false));
        assertThat(ExtraFieldPropagation.getAll(context), equalTo(baggage));
    }

    @Test
    public void should_handle_equals_characters() {
        Tracing tracing = createTracing();
        B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

        String traceInfo = "b3=0000000000000001-0000000000000002-0-0000000000000003\nextra-field={'key'='val'}";
        TraceContext context = spanConverter.deserializeTraceContext(traceInfo).get();
        Map<String, String> baggage = new HashMap<>();
        baggage.put("extra-field", "{'key'='val'}");
        assertThat(context.traceId(), equalTo(1L));
        assertThat(context.spanId(), equalTo(2L));
        assertThat(context.sampled(), equalTo(false));
        assertThat(context.parentId(), equalTo(3L));
        assertThat(ExtraFieldPropagation.getAll(context), equalTo(baggage));
    }

    @Test
    public void should_inject_required_fields() {
        Tracing tracing = createTracing();
        B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

        TraceContext context = TraceContext.newBuilder().traceId(1L).spanId(2L).parentId(3L).sampled(true).build();
        Span span = tracing.tracer().toSpan(context);
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(span.start())) {
            ExtraFieldPropagation.set("extra-field", "1");
            ExtraFieldPropagation.set("key", "val");
            String traceInfo = spanConverter.serializeTraceContext(tracing.tracer().currentSpan().context());
            assertThat(traceInfo, equalTo("b3=0000000000000001-0000000000000002-1-0000000000000003\nextra-field=1"));
        }
    }

    @Test
    public void should_inject_optional_fields() {
        Tracing tracing = createTracing();
        B3SingleFormatSpanConverter spanConverter = new B3SingleFormatSpanConverter(tracing);

        TraceContext context = TraceContext.newBuilder().traceId(1L).spanId(2L).parentId(3L).sampled(true).build();
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(tracing.tracer().toSpan(context).start())) {
            ExtraFieldPropagation.set("extra-field", "1");
            ExtraFieldPropagation.set("key", "val");
            String traceInfo = spanConverter.serializeTraceContext(tracing.tracer().currentSpan().context());
            assertThat(traceInfo, equalTo("b3=0000000000000001-0000000000000002-1-0000000000000003\nextra-field=1"));
        }
    }
}
