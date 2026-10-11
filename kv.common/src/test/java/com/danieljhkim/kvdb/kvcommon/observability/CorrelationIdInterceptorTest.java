package com.danieljhkim.kvdb.kvcommon.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class CorrelationIdInterceptorTest {

    private static final String MDC_KEY = "correlationId";

    private final CorrelationIdInterceptor interceptor = new CorrelationIdInterceptor();

    @BeforeEach
    @AfterEach
    void clearThreadState() {
        MDC.remove(MDC_KEY);
        CorrelationIds.clear();
    }

    @Test
    void interleavedCallsOnOneThreadObserveTheirOwnId() {
        String idA = UUID.randomUUID().toString();
        String idB = UUID.randomUUID().toString();
        Probe probeA = new Probe();
        Probe probeB = new Probe();

        ServerCall.Listener<String> a = intercept(idA, probeA);
        ServerCall.Listener<String> b = intercept(idB, probeB);

        a.onHalfClose();
        assertEquals(idA, probeA.halfCloseMdc);
        assertEquals(idA, probeA.halfCloseCurrent);

        a.onComplete();
        b.onHalfClose();
        assertEquals(idB, probeB.halfCloseMdc);
        assertEquals(idB, probeB.halfCloseCurrent);

        b.onMessage("x");
        assertEquals(idB, probeB.messageMdc);
        b.onReady();
        assertEquals(idB, probeB.readyMdc);

        assertNull(MDC.get(MDC_KEY));
        assertNull(CorrelationIds.current());
    }

    @Test
    void callbackOnAnotherThreadGetsCorrectIdAndLeavesThatThreadUntouched() throws Exception {
        String id = UUID.randomUUID().toString();
        Probe probe = new Probe();
        ServerCall.Listener<String> listener = intercept(id, probe);
        AtomicReference<String> afterMdc = new AtomicReference<>();
        AtomicReference<String> afterCurrent = new AtomicReference<>();
        AtomicReference<String> beforeMdc = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            beforeMdc.set(MDC.get(MDC_KEY));
            listener.onHalfClose();
            afterMdc.set(MDC.get(MDC_KEY));
            afterCurrent.set(CorrelationIds.current());
        });
        worker.start();
        worker.join();

        assertNull(beforeMdc.get());
        assertEquals(id, probe.halfCloseMdc);
        assertEquals(id, probe.halfCloseCurrent);
        assertNull(afterMdc.get());
        assertNull(afterCurrent.get());
        assertNull(MDC.get(MDC_KEY));
        assertNull(CorrelationIds.current());
    }

    @Test
    void callbacksRestorePreviousStateIncludingOnCompleteAndCancel() {
        String id = UUID.randomUUID().toString();
        ServerCall.Listener<String> listener = intercept(id, new Probe());
        MDC.put(MDC_KEY, "outer-mdc");
        CorrelationIds.set("outer-current");

        listener.onMessage("x");
        assertOuterState();
        listener.onHalfClose();
        assertOuterState();
        listener.onReady();
        assertOuterState();
        listener.onCancel();
        assertOuterState();
        listener.onComplete();
        assertOuterState();
    }

    @Test
    void callbackExceptionsRestorePreviousState() {
        String id = UUID.randomUUID().toString();
        RuntimeException failure = new IllegalStateException("boom");
        ServerCall.Listener<String> listener = intercept(id, new ServerCall.Listener<>() {
            @Override
            public void onMessage(String message) {
                throw failure;
            }

            @Override
            public void onHalfClose() {
                throw failure;
            }

            @Override
            public void onReady() {
                throw failure;
            }

            @Override
            public void onCancel() {
                throw failure;
            }

            @Override
            public void onComplete() {
                throw failure;
            }
        });
        MDC.put(MDC_KEY, "outer-mdc");
        CorrelationIds.set("outer-current");

        List<Runnable> callbacks = List.of(
                () -> listener.onMessage("x"),
                listener::onHalfClose,
                listener::onReady,
                listener::onCancel,
                listener::onComplete);
        for (Runnable callback : callbacks) {
            assertEquals(failure, assertThrows(RuntimeException.class, callback::run));
            assertOuterState();
        }
    }

    @Test
    void startCallExceptionRestoresPreviousState() {
        MDC.put(MDC_KEY, "outer-mdc");
        CorrelationIds.set("outer-current");
        ServerCallHandler<String, String> failing = (call, headers) -> {
            assertNotEquals("outer-mdc", MDC.get(MDC_KEY));
            throw new IllegalStateException("startCall failed");
        };

        assertThrows(
                IllegalStateException.class, () -> interceptor.interceptCall(new StubCall(), new Metadata(), failing));
        assertOuterState();
    }

    @Test
    void startCallExceptionWithNoPriorStateLeavesNothingBehind() {
        ServerCallHandler<String, String> failing = (call, headers) -> {
            throw new IllegalStateException("startCall failed");
        };

        assertThrows(
                IllegalStateException.class, () -> interceptor.interceptCall(new StubCall(), new Metadata(), failing));
        assertNull(MDC.get(MDC_KEY));
        assertNull(CorrelationIds.current());
    }

    @Test
    void invalidHeaderIsReplacedWithGeneratedUuid() {
        Metadata headers = new Metadata();
        headers.put(CorrelationIdInterceptor.HEADER, "not-a-uuid\nforged");
        Probe probe = new Probe();

        ServerCall.Listener<String> listener = interceptor.interceptCall(new StubCall(), headers, (call, h) -> probe);
        listener.onHalfClose();

        assertNotEquals("not-a-uuid\nforged", probe.halfCloseMdc);
        assertEquals(probe.halfCloseMdc, UUID.fromString(probe.halfCloseMdc).toString());
    }

    private static void assertOuterState() {
        assertEquals("outer-mdc", MDC.get(MDC_KEY));
        assertEquals("outer-current", CorrelationIds.current());
    }

    private ServerCall.Listener<String> intercept(String correlationId, ServerCall.Listener<String> delegate) {
        Metadata headers = new Metadata();
        headers.put(CorrelationIdInterceptor.HEADER, correlationId);
        return interceptor.interceptCall(new StubCall(), headers, (call, h) -> delegate);
    }

    private static final class Probe extends ServerCall.Listener<String> {
        String halfCloseMdc;
        String halfCloseCurrent;
        String messageMdc;
        String readyMdc;

        @Override
        public void onHalfClose() {
            halfCloseMdc = MDC.get(MDC_KEY);
            halfCloseCurrent = CorrelationIds.current();
        }

        @Override
        public void onMessage(String message) {
            messageMdc = MDC.get(MDC_KEY);
        }

        @Override
        public void onReady() {
            readyMdc = MDC.get(MDC_KEY);
        }
    }

    private static final class StubCall extends ServerCall<String, String> {
        private static final MethodDescriptor<String, String> METHOD = MethodDescriptor.<String, String>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("kvdb.Test/Call")
                .setRequestMarshaller(new StringMarshaller())
                .setResponseMarshaller(new StringMarshaller())
                .build();

        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(String message) {}

        @Override
        public void close(Status status, Metadata trailers) {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<String, String> getMethodDescriptor() {
            return METHOD;
        }
    }

    private static final class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public InputStream stream(String value) {
            return new ByteArrayInputStream(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String parse(InputStream stream) {
            try {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
