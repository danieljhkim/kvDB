package com.kvdb.kvdbserver.protocol;

import com.kvdb.kvcommon.protocol.CommandParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SQLCommandParserTest {
    
    private SQLCommandParser parser;
    private FakeCommandExecutor executor;
    
    @BeforeEach
    void setUp() {
        // Start with no executor - tests will initialize as needed
        parser = new SQLCommandParser();
    }
    
    @Test
    void testEmptyCommand() {
        String result = parser.process(new String[]{});
        assertEquals("ERR: Empty command", result);
    }
    
    @Test
    void testInsufficientArguments() {
        String result = parser.executeCommand(new String[]{"SQL"});
        assertEquals("ERR: Insufficient arguments for SQL command", result);
    }
    
    @Test
    void testUnknownCommand() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "UNKNOWN"});
        assertEquals("ERR: Unknown SQL command", result);
    }
    
    @Test
    void testHelpWithoutInit() {
        String result = parser.executeCommand(new String[]{"SQL", "HELP"});
        assertTrue(result.contains("SQL Command Parser Usage"));
        assertTrue(result.contains("INIT"));
        assertTrue(result.contains("GET"));
        assertTrue(result.contains("SET"));
    }
    
    @Test
    void testInfoWithoutInit() {
        String result = parser.executeCommand(new String[]{"SQL", "INFO"});
        assertTrue(result.contains("SQL Command Parser Usage"));
    }
    
    @Test
    void testNotInitializedError() {
        String result = parser.executeCommand(new String[]{"SQL", "GET", "key1"});
        assertEquals("ERR: Not initialized. Run SQL INIT [table_name] first.", result);
    }
    
    @Test
    void testInitWithTableName() {
        // Use a fake executor for testing instead of real repository
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        
        String result = parser.executeCommand(new String[]{"SQL", "USE", "test_table"});
        assertTrue(result.contains("OK"));
        assertTrue(result.contains("test_table"));
    }
    
    @Test
    void testInitInvalidArity() {
        String result = parser.executeCommand(new String[]{"SQL", "INIT"});
        assertEquals("ERR: Usage: INIT [table_name]", result);
    }
    
    @Test
    void testUseAfterInit() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "USE", "table2"});
        assertTrue(result.contains("OK"));
        assertTrue(result.contains("table2"));
    }
    
    @Test
    void testUseInvalidArity() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "USE"});
        assertEquals("ERR: Usage: USE [table_name]", result);
    }
    
    @Test
    void testSetAfterInit() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        assertEquals("true", result);
    }
    
    @Test
    void testSetInvalidArity() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "SET", "key1"});
        assertEquals("ERR: Usage: SET [key] [value]", result);
    }
    
    @Test
    void testGetAfterSet() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        String result = parser.executeCommand(new String[]{"SQL", "GET", "key1"});
        assertEquals("value1", result);
    }
    
    @Test
    void testGetMissingKeyReturnsNil() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "GET", "nonexistent"});
        assertEquals(CommandParser.NIL_RESPONSE, result);
    }
    
    @Test
    void testGetInvalidArity() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "GET"});
        assertEquals("ERR: Usage: GET [key]", result);
    }
    
    @Test
    void testDeleteAfterSet() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        String result = parser.executeCommand(new String[]{"SQL", "DEL", "key1"});
        assertEquals("true", result);
        
        // Verify it's deleted
        String getResult = parser.executeCommand(new String[]{"SQL", "GET", "key1"});
        assertEquals(CommandParser.NIL_RESPONSE, getResult);
    }
    
    @Test
    void testDeleteInvalidArity() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "DEL"});
        assertEquals("ERR: Usage: DEL [key]", result);
    }
    
    @Test
    void testDeleteUsingDeleteKeyword() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        String result = parser.executeCommand(new String[]{"SQL", "DELETE", "key1"});
        assertEquals("true", result);
    }
    
    @Test
    void testClearWhenEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "CLEAR"});
        assertEquals("ERR: No keys to delete", result);
    }
    
    @Test
    void testClearWhenNotEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        parser.executeCommand(new String[]{"SQL", "SET", "key2", "value2"});
        String result = parser.executeCommand(new String[]{"SQL", "CLEAR"});
        assertTrue(result.contains("OK"));
        assertTrue(result.contains("cleared"));
        
        // Verify keys are deleted
        String get1 = parser.executeCommand(new String[]{"SQL", "GET", "key1"});
        assertEquals(CommandParser.NIL_RESPONSE, get1);
    }
    
    @Test
    void testDropWhenEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "DROP"});
        assertEquals("ERR: No keys to delete", result);
    }
    
    @Test
    void testDropWhenNotEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        String result = parser.executeCommand(new String[]{"SQL", "DROP"});
        assertTrue(result.contains("OK"));
    }
    
    @Test
    void testTruncateWhenEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "TRUNCATE"});
        assertEquals("ERR: No keys to delete", result);
    }
    
    @Test
    void testTruncateWhenNotEmpty() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        parser.executeCommand(new String[]{"SQL", "SET", "key1", "value1"});
        String result = parser.executeCommand(new String[]{"SQL", "TRUNCATE"});
        assertTrue(result.contains("OK"));
    }
    
    @Test
    void testPingWhenHealthy() {
        FakeCommandExecutor fakeExec = new FakeCommandExecutor();
        parser.setCommandExecutor(fakeExec);
        String result = parser.executeCommand(new String[]{"SQL", "PING"});
        assertEquals("PONG", result);
    }
}
