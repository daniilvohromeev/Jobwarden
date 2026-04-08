package org.jobgovernance.observability.otel;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;

public final class JgkTracing {

    private final Tracer tracer;

    public JgkTracing(Tracer tracer) {
        this.tracer = tracer;
    }

    public Span startSchedulingSpan(String jobKey) {
        return tracer.spanBuilder("jgk.schedule.evaluate")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("jgk.job.key", jobKey)
                .startSpan();
    }

    public Span startClaimSpan(String jobKey, String executionId) {
        return tracer.spanBuilder("jgk.execution.claim")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("jgk.job.key", jobKey)
                .setAttribute("jgk.execution.id", executionId)
                .startSpan();
    }

    public Span startExecutionSpan(String jobKey, String executionId, int attempt) {
        return tracer.spanBuilder("jgk.execution.run")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("jgk.job.key", jobKey)
                .setAttribute("jgk.execution.id", executionId)
                .setAttribute("jgk.execution.attempt", attempt)
                .startSpan();
    }
}
