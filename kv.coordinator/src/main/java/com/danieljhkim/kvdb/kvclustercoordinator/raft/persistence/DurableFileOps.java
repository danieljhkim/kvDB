package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Centralizes the durability boundary used by Raft persistence.
 *
 * <p>A durable replacement is: write the temporary file, force its contents and metadata, atomically rename it,
 * then force the parent directory so the rename survives a power loss. Directory forcing is supported by the Linux
 * and macOS filesystems used by kvDB; an unsupported filesystem fails the write rather than silently weakening Raft's
 * stable-storage contract.
 */
public class DurableFileOps {

    public void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    public void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    public void atomicReplace(Path temporary, Path target) throws IOException {
        forceFile(temporary);
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            throw new IOException("Atomic rename is required for Raft persistence: " + target, e);
        }
        forceDirectory(target.toAbsolutePath().getParent());
    }
}
