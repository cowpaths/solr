package org.apache.solr.dvformat;

import org.apache.lucene.util.IOFunction;

import java.io.IOException;

public interface Cache<K, V> {
    boolean shouldCache(String field, int decompressedLen);
    V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction) throws IOException;
}
