package ru.yoomoney.tech.dbqueue.brave;

import brave.Request;
import brave.Span;
import brave.Tracing;
import brave.propagation.TraceContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Converts span into text and vice versa.
 * <p>
 * Serialization is built upon brave format for messaging {@link brave.propagation.B3SingleFormat}
 * It contains key value pair with new line delimeter, for example
 * b3=0000000000000001-0000000000000002-0\nbaggage-bp=1.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class B3SingleFormatSpanConverter {

    @Nonnull
    private final Tracing tracing;

    B3SingleFormatSpanConverter(@Nonnull Tracing tracing) {
        this.tracing = Objects.requireNonNull(tracing);
    }

    /**
     * Object represenation of tracing data for serialization/deserialization.
     * <p>
     * The class inherits {@link brave.Request} and acts like
     * "request to messaging system": {@link brave.propagation.B3SingleFormat}
     */
    private static final class TraceInfo extends Request {
        private final Map<String, String> data = new LinkedHashMap<>();

        private TraceInfo() {
        }

        private boolean isEmpty() {
            return data.isEmpty();
        }

        private void put(String key, String value) {
            data.put(key, value);
        }

        private String get(String key) {
            return data.get(key);
        }

        /**
         * Deserialize tracing data from text
         *
         * @param val text
         * @return tracing data
         */
        static Optional<TraceInfo> fromString(String val) {
            if (val == null || val.isEmpty()) {
                return Optional.empty();
            }
            TraceInfo traceInfo = new TraceInfo();
            String[] split = val.split("\n");
            Arrays.asList(split).forEach(keyvalPair -> {
                String[] kv = keyvalPair.split("=", 2);
                if (kv.length != 2) {
                    return;
                }
                traceInfo.put(kv[0], kv[1]);
            });
            if (traceInfo.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(traceInfo);
        }

        /**
         * Serialize tracing data into key-value format
         *
         * @return tracing data
         */
        String asString() {
            StringJoiner stringJoiner = new StringJoiner("\n");
            data.forEach((key, val) -> stringJoiner.add(key + "=" + val));
            return stringJoiner.toString();
        }

        @Override
        public Span.Kind spanKind() {
            return Span.Kind.PRODUCER;
        }

        @Override
        public Object unwrap() {
            return this;
        }
    }

    /**
     * Deserialize tracing data from string
     *
     * @param traceInfo tracing data in key value format
     * @return trace context
     */
    @Nonnull
    public Optional<TraceContext> deserializeTraceContext(@Nullable String traceInfo) {
        TraceContext.Extractor<TraceInfo> extractor = tracing.propagation().extractor(TraceInfo::get);
        return TraceInfo.fromString(traceInfo).map(tr -> extractor.extract(tr).context());
    }

    /**
     * Serialize tracing data to string
     *
     * @param traceContext trace context
     * @return tracing data in key value format
     */
    @Nonnull
    public String serializeTraceContext(@Nonnull TraceContext traceContext) {
        Objects.requireNonNull(traceContext);
        TraceContext.Injector<TraceInfo> injector = tracing.propagation().injector(TraceInfo::put);
        TraceInfo traceInfo = new TraceInfo();
        injector.inject(traceContext, traceInfo);
        return traceInfo.asString();
    }

}
