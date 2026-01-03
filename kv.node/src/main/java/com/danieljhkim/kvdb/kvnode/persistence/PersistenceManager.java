package com.danieljhkim.kvdb.kvnode.persistence;

import java.io.IOException;

public interface PersistenceManager<T> extends AutoCloseable {
    void save(T data) throws IOException;

    T load() throws IOException;

    void close() throws IOException;
}
