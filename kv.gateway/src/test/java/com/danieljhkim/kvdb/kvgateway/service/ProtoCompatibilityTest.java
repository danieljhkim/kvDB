package com.danieljhkim.kvdb.kvgateway.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.kvdb.proto.kvstore.KeyRequest;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ProtoCompatibilityTest {

    @Test
    void legacyStringClientPayloadParsesAsIdenticalBytes() throws Exception {
        String legacyKey = "legacy-\u03c0";
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream oldClient = CodedOutputStream.newInstance(bytes);
        oldClient.writeString(1, legacyKey);
        oldClient.flush();

        KeyRequest parsed = KeyRequest.parseFrom(bytes.toByteArray());

        assertArrayEquals(
                legacyKey.getBytes(StandardCharsets.UTF_8), parsed.getKey().toByteArray());
        assertEquals(1, KeyRequest.getDescriptor().findFieldByName("key").getNumber());
        assertEquals(
                Descriptors.FieldDescriptor.Type.BYTES,
                KeyRequest.getDescriptor().findFieldByName("key").getType());
    }

    @Test
    void unknownFieldsSurviveParseAndReserialization() throws Exception {
        PutRequest known = PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setValue(ByteString.copyFromUtf8("value"))
                .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(known.toByteArray());
        CodedOutputStream futureClient = CodedOutputStream.newInstance(bytes);
        futureClient.writeUInt32(99, 7);
        futureClient.flush();

        PutRequest parsed = PutRequest.parseFrom(bytes.toByteArray());
        PutRequest reparsed = PutRequest.parseFrom(parsed.toByteArray());

        assertTrue(parsed.getUnknownFields().hasField(99));
        assertEquals(parsed.getUnknownFields(), reparsed.getUnknownFields());
    }

    @Test
    void optionalCasPresenceKeepsTheExistingVarintWireShape() throws Exception {
        assertFalse(WriteOptions.getDefaultInstance().hasIfVersionEquals());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream oldWire = CodedOutputStream.newInstance(bytes);
        oldWire.writeUInt64(4, 0);
        oldWire.flush();

        WriteOptions parsed = WriteOptions.parseFrom(bytes.toByteArray());

        assertTrue(parsed.hasIfVersionEquals());
        assertEquals(0, parsed.getIfVersionEquals());
        assertEquals(
                4,
                WriteOptions.getDescriptor()
                        .findFieldByName("if_version_equals")
                        .getNumber());
    }
}
