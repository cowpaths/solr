/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.dvformat;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.BiFunction;

public class Caching90DocValuesFormat extends DocValuesFormat {

  private final Lucene90DocValuesFormat backing = new Lucene90DocValuesFormat();

  private final Map<Object, Cache<CompressedBlockEntry, DecompressedBlockValue>[]> caches = new WeakHashMap<>();

  public Caching90DocValuesFormat() {
    this("Caching90");
  }

  public Caching90DocValuesFormat(String serviceName) {
    super(serviceName);
  }

  public Cache<?, ?> getCache(Object key) {
    Cache<CompressedBlockEntry, DecompressedBlockValue>[] cacheHolder = caches.get(key);
    return cacheHolder == null ? null : cacheHolder[0];
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void addCache(Object key, Cache<?, ?> cache) {
    this.caches.computeIfAbsent(key, (k) -> new Cache[1])[0] = (Cache<CompressedBlockEntry, DecompressedBlockValue>) cache;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return backing.fieldsConsumer(state);
  }

  public static <K, V> Cache<K, V> configure(DirectoryReader r, Map<String, Object> config, BiFunction<Set<String>, Integer, Cache<K, V>> wrappingFunction) {
    String tmp = (String) config.get("fields");
    final Set<String> fields;
    if (tmp == null || "*".equals(tmp)) {
      fields = null;
    } else {
      fields = Set.of(tmp.split("\\s*,\\s*"));
    }
    tmp = (String) config.get("minDecompressedLen");
    int minDecompressedLen = tmp == null ? 0 : Integer.parseInt(tmp);
    Cache<K, V> cache = wrappingFunction.apply(fields, minDecompressedLen);
    IdentityHashMap<Object, Set<Caching90DocValuesFormat>> perCoreKey = new IdentityHashMap<>();
    for (LeafReaderContext ctx : r.leaves()) {
      LeafReader leafReader = FilterLeafReader.unwrap(ctx.reader());
      if (leafReader instanceof SegmentReader) {
        SegmentInfo si = ((SegmentReader) leafReader).getSegmentInfo().info;
        DocValuesFormat docValuesFormat = si.getCodec().docValuesFormat();
        Object coreKey = si.dir;
        Set<Caching90DocValuesFormat> dvfs = perCoreKey.get(coreKey);
        boolean createdDvfs = false;
        if (dvfs == null) {
          createdDvfs = true;
          dvfs = Collections.newSetFromMap(new IdentityHashMap<>());
        }
        if (docValuesFormat instanceof Caching90DocValuesFormat) {
          dvfs.add((Caching90DocValuesFormat) docValuesFormat);
        } else if (docValuesFormat instanceof PerFieldDocValuesFormat) {
          Collection<String> allFields;
          if (fields != null) {
            allFields = fields;
          } else {
            FieldInfos fieldInfos = leafReader.getFieldInfos();
            allFields = new ArrayList<>(fieldInfos.size());
            for (FieldInfo fi : fieldInfos) {
              switch (fi.getDocValuesType()) {
                case SORTED:
                case SORTED_SET:
                  allFields.add(fi.name);
                  break;
                default:
                  // we don't care about these because they don't have TermsDict
                  break;
              }
            }
          }
          for (String field : allFields) {
            DocValuesFormat dvf = ((PerFieldDocValuesFormat) docValuesFormat).getDocValuesFormatForField(field);
            if (dvf instanceof Caching90DocValuesFormat) {
              dvfs.add((Caching90DocValuesFormat) dvf);
            }
          }
        }
        if (createdDvfs && !dvfs.isEmpty()) {
          perCoreKey.put(coreKey, dvfs);
        }
      }
    }
    for (Map.Entry<Object, Set<Caching90DocValuesFormat>> e : perCoreKey.entrySet()) {
      Object coreKey = e.getKey();
      for (Caching90DocValuesFormat dvf : e.getValue()) {
        dvf.addCache(coreKey, cache);
      }
    }
    return cache;
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    SegmentReadState.Decompressor decompressor = state.decompressor;
    String segmentName = state.segmentInfo.name;
    @SuppressWarnings({"unchecked", "rawtypes"})
    Cache<CompressedBlockEntry, DecompressedBlockValue>[] cacheHolder = caches.computeIfAbsent(state.segmentInfo.dir, (k) -> new Cache[1]);
    state = new SegmentReadState(state, new SegmentReadState.Decompressor() {
      @Override
      public int decompress(FieldInfo field, long offset, IndexInput compressed, int decompressedLen, byte[] dest, int dOff) throws IOException {
        Cache<CompressedBlockEntry, DecompressedBlockValue> cache = cacheHolder[0];
        if (cache == null || !cache.shouldCache(field.name, decompressedLen)) {
          return decompressor.decompress(field, offset, compressed, decompressedLen, dest, dOff);
        }
        CompressedBlockEntry key = new CompressedBlockEntry(segmentName, field.name, field.getDocValuesGen(), offset);
        boolean[] cached = new boolean[]{true};
        DecompressedBlockValue decompressed = cache.computeIfAbsent(key, (k) -> {
          cached[0] = false;
          int check = decompressor.decompress(field, offset, compressed, decompressedLen, dest, dOff);
          assert check == decompressedLen;
          byte[] ret = new byte[decompressedLen];
          System.arraycopy(dest, dOff, ret, 0, decompressedLen);
          return new DecompressedBlockValue(ret, compressed.getFilePointer());
        });
        if (cached[0]) {
          System.arraycopy(decompressed.bytes, 0, dest, dOff, decompressedLen);
          compressed.seek(decompressed.compressedBlockEnd);
        }
        return decompressedLen;
      }
    });
    return new CachingDocValuesProducer(backing.fieldsProducer(state));
  }

  private static final class CompressedBlockEntry {
    private final String segmentName;
    private final String fieldName;
    private final long dvGen;
    private final long offset;

    private CompressedBlockEntry(String segmentName, String fieldName, long dvGen, long offset) {
      this.segmentName = segmentName;
      this.fieldName = fieldName;
      this.dvGen = dvGen;
      this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CompressedBlockEntry that = (CompressedBlockEntry) o;
      return dvGen == that.dvGen && offset == that.offset && segmentName.equals(that.segmentName) && fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(segmentName, fieldName, dvGen, offset);
    }
  }

  private static final class DecompressedBlockValue {
    private final byte[] bytes;
    private final long compressedBlockEnd;

    private DecompressedBlockValue(byte[] bytes, long compressedBlockEnd) {
      this.bytes = bytes;
      this.compressedBlockEnd = compressedBlockEnd;
    }
  }

  private static final class CachingDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer backing;

    private CachingDocValuesProducer(DocValuesProducer backing) {
      this.backing = backing;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      return backing.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      return backing.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      return backing.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      return backing.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      return backing.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
      backing.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
      backing.close();
    }
  }
}
