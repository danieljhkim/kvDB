package com.danieljhkim.kvdb.kvcommon.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Resource lookup is exercised against a temporary directory used as the classpath root, which mirrors how a packaged
 * jar keeps application.properties at its root. The repository does not ship a root fixture, so no test resource
 * changes the classpath for sibling tests.
 */
class SystemConfigTest {

    private static final String ENV = "root-test";
    private final Map<String, String> originalProperties = new HashMap<>();

    @TempDir
    Path classpathRoot;

    @TempDir
    Path filesystemRoot;

    @BeforeEach
    void isolateProperties() {
        for (String key : new String[] {"kvdb.env", "kvdb.server.port", "kvdb.kvdb.server.port"}) {
            originalProperties.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
    }

    @AfterEach
    void restoreProperties() {
        originalProperties.forEach((key, value) -> {
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvdb.server.port", "server.port"})
    void overridesUseOnePrefixWithJvmThenEnvironmentThenFilePrecedence(String key) throws IOException {
        write(filesystemRoot, "application.properties", "kvdb.server.port=7000\n");
        Map<String, String> environment = new HashMap<>();
        environment.put("KVDB_SERVER_PORT", "8124");
        environment.put("KVDB_KVDB_SERVER_PORT", "9999");
        System.setProperty("kvdb.kvdb.server.port", "9998");
        SystemConfig config = fromFilesystem(environment);

        System.setProperty("kvdb.server.port", "8123");
        assertEquals("8123", config.getProperty(key, "fallback"));

        System.clearProperty("kvdb.server.port");
        assertEquals("8124", config.getProperty(key, "fallback"));

        System.setProperty("kvdb.server.port", "");
        assertEquals("8124", config.getProperty(key, "fallback"));

        environment.put("KVDB_SERVER_PORT", "");
        assertEquals("7000", config.getProperty(key, "fallback"));
        environment.remove("KVDB_SERVER_PORT");
        assertEquals("7000", config.getProperty(key, "fallback"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvdb.server.port", "server.port"})
    void bothKeyFormsFallBackToClasspathThenDefault(String key) throws IOException {
        write(classpathRoot, "isolated/application.properties", "kvdb.server.port=7200\n");
        SystemConfig config;
        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            config = new SystemConfig("isolated", loader, Map.<String, String>of()::get);
        }
        assertEquals("7200", config.getProperty(key, "fallback"));

        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            config = new SystemConfig(filesystemRoot.toString(), loader, Map.<String, String>of()::get);
        }
        assertEquals("fallback", config.getProperty(key, "fallback"));
        assertNull(config.getProperty(key));
    }

    @Test
    void unprefixedFileKeysRemainSupportedAndCanonicalKeysTakePrecedence() throws IOException {
        write(filesystemRoot, "application.properties", "server.port=7300\n");
        assertEquals("7300", fromFilesystem(Map.of()).getProperty("server.port"));

        write(filesystemRoot, "application.properties", "server.port=7300\nkvdb.server.port=7400\n");
        assertEquals("7400", fromFilesystem(Map.of()).getProperty("server.port"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvdb.database.default.table", "database.default.table"})
    void databaseTableLookupUsesCanonicalFileKey(String key) throws IOException {
        write(filesystemRoot, "application.properties", "kvdb.database.default.table=custom_store\n");
        assertEquals("custom_store", fromFilesystem(Map.of()).getProperty(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kvdb.database.default.pool.minIdle", "database.default.pool.minIdle"})
    void environmentNamesDoNotDependOnDefaultLocale(String key) throws IOException {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            SystemConfig config = fromFilesystem(Map.of("KVDB_DATABASE_DEFAULT_POOL_MINIDLE", "8"));
            assertEquals("8", config.getProperty(key, "5"));
        } finally {
            Locale.setDefault(originalLocale);
        }
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
            config = new SystemConfig(filesystemRoot.toString(), loader, Map.<String, String>of()::get);
        }

        assertEquals("7500", config.getProperty("kvdb.server.port"));
    }

    private SystemConfig fromClasspath(String resourcePath) throws IOException {
        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            return new SystemConfig(resourcePath, loader, Map.<String, String>of()::get);
        }
    }

    private SystemConfig fromFilesystem(Map<String, String> environment) throws IOException {
        try (URLClassLoader loader = classLoaderFor(classpathRoot)) {
            return new SystemConfig(filesystemRoot.toString(), loader, environment::get);
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
