package com.kvdb.kvclustercoordinator.protocol;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KVCommandParserTest {

    private KVCommandParser parser;
    private FakeCommandExecutor executor;

    @BeforeEach
    void setUp() {
        parser = new KVCommandParser();
        executor = new FakeCommandExecutor();
    }

    @Test
    void testEmptyCommand() {
        String result = parser.executeCommand(new String[]{}, executor);
        assertEquals("ERR: Empty command", result);
    }

    @Test
    void testUnknownCommand() {
        String result = parser.executeCommand(new String[]{"UNKNOWN"}, executor);
        assertEquals("ERR: Unknown command", result);
    }

    @Test
    void testHelp() {
        String result = parser.executeCommand(new String[]{"HELP"}, executor);
        assertTrue(result.contains("KV Command Parser Usage"));
        assertTrue(result.contains("SET"));
        assertTrue(result.contains("GET"));
    }

    @Test
    void testInfo() {
        String result = parser.executeCommand(new String[]{"INFO"}, executor);
        assertTrue(result.contains("KV Command Parser Usage"));
    }

    @Test
    void testSetSuccess() {
        String result = parser.executeCommand(new String[]{"SET", "key1", "value1"}, executor);
        assertEquals("true", result);
        assertEquals("value1", executor.get("key1"));
    }

    @Test
    void testSetInvalidArity() {
        String result = parser.executeCommand(new String[]{"SET", "key1"}, executor);
        assertEquals("ERR: Usage: SET key value", result);
    }

    @Test
    void testGetSuccess() {
        executor.put("key1", "value1");
        String result = parser.executeCommand(new String[]{"GET", "key1"}, executor);
        assertEquals("value1", result);
    }

    @Test
    void testGetMissingKeyReturnsNil() {
        String result = parser.executeCommand(new String[]{"GET", "nonexistent"}, executor);
        assertEquals("(nil)", result);
    }

    @Test
    void testGetInvalidArity() {
        String result = parser.executeCommand(new String[]{"GET"}, executor);
        assertEquals("ERR: Usage: GET key", result);
    }

    @Test
    void testDeleteSuccess() {
        executor.put("key1", "value1");
        String result = parser.executeCommand(new String[]{"DEL", "key1"}, executor);
        assertEquals("true", result);
        assertNull(executor.get("key1"));
    }

    @Test
    void testDeleteNonexistent() {
        String result = parser.executeCommand(new String[]{"DEL", "nonexistent"}, executor);
        assertEquals("false", result);
    }

    @Test
    void testDeleteInvalidArity() {
        String result = parser.executeCommand(new String[]{"DEL"}, executor);
        assertEquals("ERR: Usage: DEL key", result);
    }

    @Test
    void testPing() {
        String result = parser.executeCommand(new String[]{"PING"}, executor);
        assertEquals("PONG", result);
    }

    @Test
    void testShutdownInvalidArity() {
        String result = parser.executeCommand(new String[]{"SHUTDOWN"}, executor);
        assertEquals("ERR: Usage: SHUTDOWN node_id", result);
    }

    @Test
    void testShutdownWithNodeId() {
        String result = parser.executeCommand(new String[]{"SHUTDOWN", "node1"}, executor);
        // Should throw UnsupportedOperationException and return error
        assertTrue(result.contains("ERR"));
    }
}