package com.danieljhkim.kvdb.kvcommon.observability;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;

/** Small dependency-free Prometheus text registry. Labels are fixed by server code, never request data. */
public final class Metrics {

    private static final Map<String, LongAdder> COUNTERS = new ConcurrentHashMap<>();
    private static final Map<String, DoubleAdder> SUMS = new ConcurrentHashMap<>();
    private static final Map<String, LongAdder> COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, DoubleSupplier> GAUGES = new ConcurrentHashMap<>();

    private Metrics() {}

    public static void increment(String name, String service, String method, String outcome) {
        COUNTERS.computeIfAbsent(key(name, service, method, outcome), ignored -> new LongAdder())
                .increment();
    }

    public static void observe(String name, String service, String method, double value) {
        String key = key(name, service, method, "");
        SUMS.computeIfAbsent(key, ignored -> new DoubleAdder()).add(value);
        COUNTS.computeIfAbsent(key, ignored -> new LongAdder()).increment();
    }

    public static void gauge(String name, String service, DoubleSupplier supplier) {
        GAUGES.put(name + "{service=\"" + service + "\"}", supplier);
    }

    public static String scrape() {
        StringBuilder output = new StringBuilder();
        COUNTERS.forEach((key, counter) ->
                output.append(key).append(' ').append(counter.sum()).append('\n'));
        SUMS.forEach((key, sum) -> {
            output.append(sampleName(key, "_sum")).append(' ').append(sum.sum()).append('\n');
            LongAdder count = COUNTS.get(key);
            output.append(sampleName(key, "_count"))
                    .append(' ')
                    .append(count == null ? 0 : count.sum())
                    .append('\n');
        });
        GAUGES.forEach((key, gauge) ->
                output.append(key).append(' ').append(gauge.getAsDouble()).append('\n'));
        return output.toString();
    }

    private static String sampleName(String key, String suffix) {
        int labelsStart = key.indexOf('{');
        return labelsStart < 0 ? key + suffix : key.substring(0, labelsStart) + suffix + key.substring(labelsStart);
    }

    private static String key(String name, String service, String method, String outcome) {
        String labels = "service=\"" + service + "\",method=\"" + method + "\"";
        if (!outcome.isEmpty()) {
            labels += ",outcome=\"" + outcome + "\"";
        }
        return name + "{" + labels + "}";
    }
}
