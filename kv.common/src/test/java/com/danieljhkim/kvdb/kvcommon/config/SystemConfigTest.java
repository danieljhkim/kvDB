package com.danieljhkim.kvdb.kvcommon.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Resource lookup is exercised against a temporary directory used as the classpath root, which mirrors how a packaged
 * jar keeps application.properties at its root. The repository does not ship a root fixture, so no test resource
 * changes the classpath for sibling tests.
 */
class SystemConfigTest {

    private static final String ENV = "root-test";

    @TempDir
    Path classpathRoot;

    @TempDir
    Path filesystemRoot;

    @AfterEach
    void clearEnv() {
        System.clearProperty("kvdb.env");
    }

    @Test
    void loadsRootClasspathDefaultWithoutFilesystemConfig() throws IOException {
        Assumptions.assumeFalse(Files.exists(Path.of("application.properties")));
        write(classpathRoot, "application.properties", "kvdb.server.port=7000\n");

        SystemConfig config = fromClasspath("");

        assertEquals("7000", config.getProperty("kvdb.server.port"));
    }

    @Test
    void rootEnvironmentFileOverridesRootDefault() throws IOException {
        Assumptions.assumeFalse(Files.exists(Path.of("application.properties")));
        write(classpathRoot, "application.properties", "kvdb.server.port=7000\nkvdb.server.host=localhost\n");
        write(classpathRoot, "application-" + ENV + ".properties", "kvdb.server.port=7100\n");
        System.setProperty("kvdb.env", ENV);

        SystemConfig config = fromClasspath("");

        assertEquals("7100", config.getProperty("kvdb.server.port"));
        assertEquals("localhost", config.getProperty("kvdb.server.host"));
    }

    @Test
    void nestedResourcePathLoadsItsDefaultAndEnvironmentFiles() throws IOException {
        write(classpathRoot, "application.properties", "kvdb.server.port=7000\n");
        write(classpathRoot, "config/nested/application.properties", "kvdb.server.port=7200\n");
        write(classpathRoot, "config/nested/application-" + ENV + ".properties", "kvdb.server.port=7300\n");

        assertEquals("7200", fromClasspath("config/nested").getProperty("kvdb.server.port"));

        System.setProperty("kvdb.env", ENV);

        assertEquals("7300", fromClasspath("config/nested").getProperty("kvdb.server.port"));
    }

    @Test
    void filesystemDefaultTakesPrecedenceOverClasspath() throws IOException {
        write(classpathRoot, "application.properties", "kvdb.server.port=7000\n");
        write(filesystemRoot, "application.properties", "kvdb.server.port=7500\n");

        SystemConfig config;
        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            config = new SystemConfig(filesystemRoot.toString(), loader);
        }

        assertEquals("7500", config.getProperty("kvdb.server.port"));
    }

    private SystemConfig fromClasspath(String resourcePath) throws IOException {
        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            return new SystemConfig(resourcePath, loader);
        }
    }

    /** Parent is the bootstrap loader, so only the temporary root can answer resource lookups. */
    private static URLClassLoader classLoaderFor(Path root) throws IOException {
        return new URLClassLoader(new URL[] {root.toUri().toURL()}, null);
    }

    private static void write(Path root, String relativePath, String content) throws IOException {
        Path file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }
}
