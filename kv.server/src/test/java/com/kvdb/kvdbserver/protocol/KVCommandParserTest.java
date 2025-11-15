package com.kvdb.kvdbserver.protocol;

import com.kvdb.kvcommon.protocol.CommandParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KVCommandParserTest {
    
    private KVCommandParser parser;
    private FakeCommandExecutor executor;
    
    @BeforeEach
    void setUp() {
        executor = new FakeCommandExecutor();
        parser = new KVCommandParser(executor);
    }
    
    @Test
    void testEmptyCommand() {
        String result = parser.process(new String[]{});
        assertEquals("ERR: Empty command", result);
    }
    
    @Test
    void testUnknownCommand() {
        String result = parser.executeCommand(new String[]{"UNKNOWN"});
        assertEquals("ERR: Unknown command", result);
    }
    
    @Test
    void testHelp() {
        String result = parser.executeCommand(new String[]{"HELP"});
        assertTrue(result.contains("KV Command Parser Usage"));
        assertTrue(result.contains("SET"));
        assertTrue(result.contains("GET"));
    }
    
    @Test
    void testInfo() {
        String result = parser.executeCommand(new String[]{"INFO"});
        assertTrue(result.contains("KV Command Parser Usage"));
    }
    
    @Test
    void testSetSuccess() {
        String result = parser.executeCommand(new String[]{"SET", "key1", "value1"});
        assertEquals("true", result);
        assertEquals("value1", executor.get("key1"));
    }
    
    @Test
    void testSetInvalidArity() {
        String result = parser.executeCommand(new String[]{"SET", "key1"});
        assertEquals("ERR: Usage: SET key value", result);
    }
    
    @Test
    void testGetSuccess() {
        executor.put("key1", "value1");
        String result = parser.executeCommand(new String[]{"GET", "key1"});
        assertEquals("value1", result);
    }
    
    @Test
    void testGetMissingKeyReturnsNil() {
        String result = parser.executeCommand(new String[]{"GET", "nonexistent"});
        assertEquals(CommandParser.NIL_RESPONSE, result);
    }
    
    @Test
    void testGetInvalidArity() {
        String result = parser.executeCommand(new String[]{"GET"});
        assertEquals("ERR: Usage: GET key", result);
    }
    
    @Test
    void testDeleteSuccess() {
        executor.put("key1", "value1");
        String result = parser.executeCommand(new String[]{"DEL", "key1"});
        assertEquals("true", result);
        assertNull(executor.get("key1"));
    }
    
    @Test
    void testDeleteNonexistent() {
        String result = parser.executeCommand(new String[]{"DEL", "nonexistent"});
        assertEquals("false", result);
    }
    
    @Test
    void testDeleteInvalidArity() {
        String result = parser.executeCommand(new String[]{"DEL"});
        assertEquals("ERR: Usage: DEL key", result);
    }
    
    @Test
    void testExistsTrue() {
        executor.put("key1", "value1");
        String result = parser.executeCommand(new String[]{"EXISTS", "key1"});
        assertEquals("1", result);
    }
    
    @Test
    void testExistsFalse() {
        String result = parser.executeCommand(new String[]{"EXISTS", "nonexistent"});
        assertEquals("0", result);
    }
    
    @Test
    void testExistsInvalidArity() {
        String result = parser.executeCommand(new String[]{"EXISTS"});
        assertEquals("ERR: Usage: EXISTS key", result);
    }
    
    @Test
    void testDropWhenEmpty() {
        String result = parser.executeCommand(new String[]{"DROP"});
        assertEquals("ERR: No keys to delete", result);
    }
    
    @Test
    void testDropWhenNotEmpty() {
        executor.put("key1", "value1");
        executor.put("key2", "value2");
        String result = parser.executeCommand(new String[]{"DROP"});
        assertEquals("OK", result);
        assertEquals(0, executor.size());
    }
    
    @Test
    void testPing() {
        String result = parser.executeCommand(new String[]{"PING"});
        assertEquals("PONG", result);
    }
    
    @Test
    void testShutdown() {
        String result = parser.executeCommand(new String[]{"SHUTDOWN"});
        assertTrue(result.contains("OK"));
    }
    
    @Test
    void testQuit() {
        String result = parser.executeCommand(new String[]{"QUIT"});
        assertTrue(result.contains("OK"));
    }
    
    @Test
    void testTerminate() {
        String result = parser.executeCommand(new String[]{"TERMINATE"});
        assertTrue(result.contains("OK"));
    }
}
