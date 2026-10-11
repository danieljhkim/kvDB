package com.danieljhkim.kvdb.kvadmin.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

class AdminLogPatternTest {

    @Test
    void configuredConsoleLayoutSeparatesEventsAndKeepsThrowableOutput() throws Exception {
        URL configUrl = getClass().getResource("/log4j2-spring.xml");
        assertNotNull(configUrl);

        LoggerContext context = new LoggerContext("AdminLogPatternTest");
        Configuration configuration = ConfigurationFactory.getInstance()
                .getConfiguration(context, "admin-log-pattern-test", configUrl.toURI(), getClass().getClassLoader());
        context.start(configuration);

        try {
            var consoleAppender = configuration.getAppender("Console");
            assertNotNull(consoleAppender);
            assertTrue(consoleAppender.getLayout() instanceof PatternLayout);

            CapturingAppender capture = new CapturingAppender(consoleAppender.getLayout());
            capture.start();
            var logger = context.getLogger("com.danieljhkim.kvdb.kvadmin.config.AdminLogPatternTest");
            logger.addAppender(capture);
            try {
                logger.info("first-event");
                logger.error("second-event", new IllegalStateException("expected throwable text"));
            } finally {
                logger.removeAppender(capture);
                capture.stop();
            }

            assertEquals(2, capture.events.size());
            String first = capture.events.get(0);
            String second = capture.events.get(1);
            assertTrue(first.matches(
                    "(?s)^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}(?:Z|[+-]\\d{2}:\\d{2}) \\[.*\\] INFO  .*AdminLogPatternTest - first-event\\R$"),
                    first);
            assertTrue(second.matches(
                    "(?s)^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}(?:Z|[+-]\\d{2}:\\d{2}) \\[.*\\] ERROR .*AdminLogPatternTest - second-event\\R.*"),
                    second);
            assertTrue(second.contains("java.lang.IllegalStateException: expected throwable text"), second);
        } finally {
            context.stop();
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> events = new ArrayList<>();

        private CapturingAppender(org.apache.logging.log4j.core.Layout<? extends Serializable> layout) {
            super("admin-log-pattern-capture", null, layout, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(getLayout().toSerializable(event).toString());
        }
    }
}
