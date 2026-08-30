package com.danieljhkim.kvdb.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.kvdb.proto.kvstore.KeyValueRequest;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class WireCompatibilityTest {

    @Test
    void binaryKeyAndValueFieldsKeepTheirPublishedNumbersAndWireType() throws Exception {
        assertField(PutRequest.getDescriptor().findFieldByName("key"), 2, Type.BYTES);
        assertField(PutRequest.getDescriptor().findFieldByName("value"), 3, Type.BYTES);
        assertField(KeyValueRequest.getDescriptor().findFieldByName("key"), 1, Type.BYTES);
        assertField(KeyValueRequest.getDescriptor().findFieldByName("value"), 2, Type.BYTES);

        byte[] binary = {(byte) 0xff, 0x00, (byte) 0x80};
        PutRequest parsed = PutRequest.parseFrom(PutRequest.newBuilder()
                .setKey(ByteString.copyFrom(binary))
                .setValue(ByteString.copyFrom(binary))
                .build()
                .toByteArray());

        assertArrayEquals(binary, parsed.getKey().toByteArray());
        assertArrayEquals(binary, parsed.getValue().toByteArray());
    }

    @Test
    void optionalCasPresenceDistinguishesAbsentFromExplicitZero() {
        WriteOptions absent = WriteOptions.getDefaultInstance();
        WriteOptions explicitZero =
                WriteOptions.newBuilder().setIfVersionEquals(0).build();
        KeyValueRequest internalExplicitZero =
                KeyValueRequest.newBuilder().setIfVersionEquals(0).build();

        assertFalse(absent.hasIfVersionEquals());
        assertTrue(explicitZero.hasIfVersionEquals());
        assertEquals(0, explicitZero.getIfVersionEquals());
        assertTrue(internalExplicitZero.hasIfVersionEquals());
    }

    @Test
    void unknownFieldsSurviveParseAndForward() throws Exception {
        PutRequest request = PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setValue(ByteString.copyFromUtf8("value"))
                .build();
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        request.writeTo(wire);
        CodedOutputStream coded = CodedOutputStream.newInstance(wire);
        coded.writeUInt64(99, 7);
        coded.flush();

        PutRequest parsed = PutRequest.parseFrom(wire.toByteArray());
        PutRequest forwarded = PutRequest.parseFrom(parsed.toByteArray());

        assertEquals(List.of(7L), forwarded.getUnknownFields().getField(99).getVarintList());
    }

    private static void assertField(com.google.protobuf.Descriptors.FieldDescriptor field, int number, Type type) {
        assertEquals(number, field.getNumber());
        assertEquals(type, field.getType());
    }
}
