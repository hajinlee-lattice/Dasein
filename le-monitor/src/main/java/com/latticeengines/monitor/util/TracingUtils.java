package com.latticeengines.monitor.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

/**
 * Util functions for distributed tracing
 */
public class TracingUtils {

    public static void finish(Span span) {
        if (span != null) {
            span.finish();
        }
    }

    public static void finishActiveSpan() {
        if (GlobalTracer.get() == null || GlobalTracer.get().activeSpan() == null) {
            return;
        }

        GlobalTracer.get().activeSpan().finish();
    }

    /**
     * Helper to retrieve current active tracing context in map representation
     *
     * @return current active tracing context, {@code null} if no active span
     */
    public static Map<String, String> getActiveTracingContext() {
        if (GlobalTracer.get() == null || GlobalTracer.get().activeSpan() == null) {
            return null;
        }

        Map<String, String> ctx = new HashMap<>();
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.activeSpan();
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(ctx));
        return ctx;
    }

    public static void logInActiveSpan(Map<String, ?> fields) {
        if (GlobalTracer.get() == null || GlobalTracer.get().activeSpan() == null || MapUtils.isEmpty(fields)) {
            return;
        }
        GlobalTracer.get().activeSpan().log(fields);
    }

    /*-
     * helpers to serialize span/context
     */
    public static Map<String, String> getTracingContext(Span span) {
        if (span == null) {
            return null;
        }

        return getTracingContext(span.context());
    }

    public static Map<String, String> getTracingContext(SpanContext spanContext) {
        if (spanContext == null || GlobalTracer.get() == null) {
            return null;
        }

        Tracer tracer = GlobalTracer.get();
        Map<String, String> tracingContext = new HashMap<>();
        tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(tracingContext));
        return tracingContext;
    }

    /*
     * helper to log error in span
     */
    public static void logError(Span span, Exception e, String msg) {
        if (span == null || GlobalTracer.get() == null) {
            return;
        }

        Map<String, Object> fields = new HashMap<>();
        fields.put(Fields.EVENT, "error");
        if (e != null) {
            fields.put(Fields.ERROR_OBJECT, e);
            fields.put(Fields.ERROR_KIND, e.getClass().getSimpleName());
        }
        if (StringUtils.isNotBlank(msg)) {
            fields.put(Fields.MESSAGE, msg);
        }
        span.setTag(Tags.ERROR, true);
        span.log(fields);
    }

    /**
     * Retrieve span context from map representation.
     *
     * @param tracingContext
     *            given text map tracing span context
     * @return parsed {@link SpanContext} object, {@code null} if no global tracer
     *         or input is invalid
     */
    public static SpanContext getSpanContext(Map<String, String> tracingContext) {
        if (MapUtils.isEmpty(tracingContext) || GlobalTracer.get() == null) {
            return null;
        }

        return GlobalTracer.get().extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(tracingContext));
    }
}
