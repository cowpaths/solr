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
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.solr.cloud.ZkDynamicProperty;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Utilities to facilitate configuration of a ngram subfield (populated at segment flush by a custom
 * PostingsFormat) that can be used to pre-filter terms that must be evaluated for
 * substring/wildcard/regex search. See {@link BloomStrField} and {@link BloomTextField}.
 */
public final class BloomUtils {

  public static final String DEFAULT_BLOOM_ANALYZER_ID = "TRIGRAM";

  public static String constructName(PostingsFormat delegate, String bloomAnalyzerId) {
    return "Bloom" + delegate.getName() + "x" + bloomAnalyzerId;
  }

  /**
   * Base suffix must start with `0` to ensure that FieldsProducer visits the subfield immediately
   * after the parent field -- i.e., the fieldnames must sort lexicographically adjacent.
   */
  private static final String BLOOM_FIELD_BASE_SUFFIX = "0Ngram";

  private static final String MAX_SUBSTRING_FIELD_BASE_SUFFIX = "_max_substring";

  /** Single-valued ngram suffix */
  private static final String BLOOM_FIELD_BASE_SUFFIX_SINGLE = BLOOM_FIELD_BASE_SUFFIX.concat("S");

  /** multi-valued ngram suffix */
  private static final String BLOOM_FIELD_BASE_SUFFIX_MULTI = BLOOM_FIELD_BASE_SUFFIX.concat("M");

  private static final int REQUIRE_LENGTH_DIFFERENTIAL = BLOOM_FIELD_BASE_SUFFIX_SINGLE.length();

  private static final int COMPLETE_TERM_SIGNAL = 0xFF;
  private static final int PREFIX_SIGNAL = 0xFE;
  private static final int SUFFIX_SIGNAL = 0xFD;
  private static final int SUBSTRING_SIGNAL = 0xFC;

  /**
   * Returns null if ngramField is _not_ an ngram field; otherwise returns the field name of the
   * ngramField's associated raw field.
   */
  public static String isNgramSubfield(String srcField, String ngramField) {
    int thisLen = ngramField.length();
    int ngramSuffixStartIdx = thisLen - REQUIRE_LENGTH_DIFFERENTIAL;
    if (srcField != null && ngramSuffixStartIdx != srcField.length()) {
      return null;
    }
    if (ngramField.indexOf(BLOOM_FIELD_BASE_SUFFIX, ngramSuffixStartIdx) == ngramSuffixStartIdx) {
      switch (ngramField.charAt(thisLen - 1)) {
        case 'S':
        case 'M':
          if (srcField == null) {
            return ngramField.substring(0, ngramSuffixStartIdx);
          } else if (ngramField.startsWith(srcField)) {
            return ngramField;
          }
      }
    }
    return null;
  }

  public enum NgramStatus {
    DISABLED,
    ENABLE_NGRAMS,
    ENABLE_MAX_SUBSTRING,
    ENABLED
  }

  private static final NgramStatus DEFAULT_ENABLE_NGRAMS =
      "false".equals(System.getProperty("enableNgrams"))
          ? NgramStatus.DISABLED
          : NgramStatus.ENABLED;

  private static final ThreadLocal<NgramStatus> ENABLE_NGRAMS =
      new ThreadLocal<>() {
        @Override
        protected NgramStatus initialValue() {
          return DEFAULT_ENABLE_NGRAMS;
        }
      };

  public static void init(SolrQueryRequest req) {
    String spec = req.getParams().get("enableNgrams");
    NgramStatus enableNgrams;
    if (spec == null) {
      enableNgrams = DEFAULT_ENABLE_NGRAMS;
    } else if ("ngramOnly".equals(spec)) {
      enableNgrams = NgramStatus.ENABLE_NGRAMS;
    } else if ("maxSubstringOnly".equals(spec)) {
      enableNgrams = NgramStatus.ENABLE_MAX_SUBSTRING;
    } else if ("false".equals(spec)) {
      enableNgrams = NgramStatus.DISABLED;
    } else if ("true".equals(spec)) {
      enableNgrams = NgramStatus.ENABLED;
    } else {
      throw new IllegalArgumentException("bad enableNgrams spec: " + spec);
    }
    ENABLE_NGRAMS.set(enableNgrams);
  }

  public static NgramStatus enableNgrams() {
    return ENABLE_NGRAMS.get();
  }

  static final Analyzer KEYWORD_ANALYZER =
      new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tk = new KeywordTokenizer();
          return new TokenStreamComponents(tk, tk);
        }
      };

  static PostingsFormat getPostingsFormat(Map<String, String> args) {
    String bloomAnalyzerId = args.remove("bloomAnalyzerId");
    String postingsFormat = args.get("postingsFormat");
    PostingsFormat pf;
    if (postingsFormat != null) {
      pf = PostingsFormat.forName(postingsFormat);
    } else {
      // start with the default postingsFormat.
      pf = Codec.getDefault().postingsFormat();
      if (pf instanceof PerFieldPostingsFormat) {
        pf = ((PerFieldPostingsFormat) pf).getPostingsFormatForField("");
      }
    }
    if (pf instanceof BloomAnalyzerSupplier) {
      if (bloomAnalyzerId != null
          && !bloomAnalyzerId.equals(((BloomAnalyzerSupplier) pf).getBloomAnalyzerId())) {
        throw new IllegalArgumentException(
            "specified `bloomAnalyzerId` "
                + bloomAnalyzerId
                + " conflicts with the `bloomAnalyzerId` of the specified `postingsFormat`");
      }
    } else {
      if (bloomAnalyzerId == null) {
        bloomAnalyzerId = DEFAULT_BLOOM_ANALYZER_ID;
      }
      pf = PostingsFormat.forName(constructName(pf, bloomAnalyzerId));
      if (!(pf instanceof BloomAnalyzerSupplier)) {
        throw new IllegalArgumentException(
            "constructed `postingsFormat` does not support ngram bloom filter: " + pf);
      }
      // replace any existing postingsFormat spec with our the constructed postingsFormat
      args.put("postingsFormat", pf.getName());
    }
    return pf;
  }

  static <T extends PostingsFormat & BloomAnalyzerSupplier> FieldType getFieldType(
      IndexSchema schema, T pf) {
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("docValues", "false");
    props.put("sortMissingLast", "true");
    props.put("termVectors", "false");
    props.put("omitNorms", "true");
    props.put("omitTermFreqAndPositions", "false");
    props.put("uninvertible", "false");
    props.put("postingsFormat", pf.getName());
    FieldType ret = new TextField();
    ret.setTypeName("ngram_bloom_filter_" + pf.getBloomAnalyzerId());
    ret.setIndexAnalyzer(KEYWORD_ANALYZER);
    ret.setQueryAnalyzer(pf.getBloomAnalyzer());
    // NOTE: we must call `setArgs()` here, as opposed to `init()`, in order to properly
    // set postingsFormat.
    ret.setArgs(schema, props);
    return ret;
  }

  static FieldType getMaxSubstringFieldType(IndexSchema schema, PostingsFormat pf) {
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("docValues", "false");
    props.put("sortMissingLast", "true");
    props.put("termVectors", "false");
    props.put("omitNorms", "true");
    props.put("omitTermFreqAndPositions", "false");
    props.put("uninvertible", "false");
    props.put("multiValued", "true");
    props.put("postingsFormat", "X".concat(pf.getName()));
    FieldType ret =
        new StrField() {
          @Override
          public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
            input = new BytesRef(input.bytes, input.offset, input.length - 1);
            switch (input.bytes[input.length] & 0xff) {
              case COMPLETE_TERM_SIGNAL:
              case PREFIX_SIGNAL:
              case SUFFIX_SIGNAL:
              case SUBSTRING_SIGNAL:
                break;
              case 0:
                input.offset += Long.BYTES + 1;
                input.length -= Long.BYTES + 1;
                break;
              default:
                throw new IllegalArgumentException();
            }
            return super.indexedToReadable(input, output);
          }
        };
    ret.setTypeName("max_substring");
    // NOTE: we must call `setArgs()` here, as opposed to `init()`, in order to properly
    // set postingsFormat.
    ret.setArgs(schema, props);
    return ret;
  }

  private static final char OMIT_NORMS = 'A';
  private static final char OMIT_TERM_FREQS_AND_POSITIONS = 'B';
  private static final char MULTI_VALUED = 'D';
  private static final char ALL_FEATURES =
      OMIT_NORMS | OMIT_TERM_FREQS_AND_POSITIONS | MULTI_VALUED;
  private static final char NO_FEATURES = ALL_FEATURES + 1;
  private static final char INITIAL = OMIT_NORMS - 1;

  static void registerDynamicSubfields(
      IndexSchema schema, FieldType bloomFieldType, FieldType maxStringFieldType) {
    for (boolean multiValued : new boolean[] {true, false}) {
      String name =
          "*" + (multiValued ? BLOOM_FIELD_BASE_SUFFIX_MULTI : BLOOM_FIELD_BASE_SUFFIX_SINGLE);
      Map<String, String> props = new HashMap<>();
      props.put("multiValued", Boolean.toString(multiValued));
      int p = SchemaField.calcProps(name, bloomFieldType, props);
      schema.registerDynamicFields(SchemaField.create(name, bloomFieldType, p, null));
    }
    if (maxStringFieldType != null) {
      String base = "*".concat(MAX_SUBSTRING_FIELD_BASE_SUFFIX);
      for (char feature = NO_FEATURES; feature >= OMIT_NORMS; feature--) {
        String name = base + feature;
        Map<String, String> props = new HashMap<>();
        props.put("omitNorms", Boolean.toString((feature & OMIT_NORMS) == OMIT_NORMS));
        props.put(
            "omitTermFreqAndPositions",
            Boolean.toString(
                (feature & OMIT_TERM_FREQS_AND_POSITIONS) == OMIT_TERM_FREQS_AND_POSITIONS));
        props.put("multiValued", Boolean.toString((feature & MULTI_VALUED) == MULTI_VALUED));
        int p = SchemaField.calcProps(name, maxStringFieldType, props);
        schema.registerDynamicFields(SchemaField.create(name, maxStringFieldType, p, null));
      }
    }
  }

  public static String getNgramFieldName(SchemaField sf) {
    return sf.getName()
        .concat(
            sf.multiValued()
                ? BloomUtils.BLOOM_FIELD_BASE_SUFFIX_MULTI
                : BloomUtils.BLOOM_FIELD_BASE_SUFFIX_SINGLE);
  }

  public static String getMaxSubstringFieldName(SchemaField sf) {
    char suffix = INITIAL;
    if (sf.omitNorms()) {
      suffix |= OMIT_NORMS;
    }
    if (sf.omitTermFreqAndPositions()) {
      suffix |= OMIT_TERM_FREQS_AND_POSITIONS;
    }
    if (sf.multiValued()) {
      suffix |= MULTI_VALUED;
    }
    if (suffix == INITIAL) {
      suffix = NO_FEATURES;
    }
    return sf.getName() + MAX_SUBSTRING_FIELD_BASE_SUFFIX + suffix;
  }

  public static Terms getNgramTerms(String rawField, FieldsProducer fp, String[] ngramTermsField)
      throws IOException {
    String ntf = rawField.concat(BLOOM_FIELD_BASE_SUFFIX_SINGLE);
    Terms ret = fp.terms(ntf);
    if (ret != null) {
      ngramTermsField[0] = ntf;
      return ret;
    }
    ntf = rawField.concat(BLOOM_FIELD_BASE_SUFFIX_MULTI);
    ret = fp.terms(ntf);
    if (ret != null) {
      ngramTermsField[0] = ntf;
    }
    return ret;
  }

  public static <T> T getMaxSubstring(
      String rawField, IOFunction<String, T> func, String[] maxSubstringField) throws IOException {
    String base = rawField.concat(MAX_SUBSTRING_FIELD_BASE_SUFFIX);
    for (char feature = NO_FEATURES; feature >= OMIT_NORMS; feature--) {
      String fieldName = base + feature;
      T ret = func.apply(fieldName);
      if (ret != null) {
        maxSubstringField[0] = fieldName;
        return ret;
      }
    }
    return null;
  }

  public interface BloomAnalyzerSupplier {
    String getBloomAnalyzerId();

    Analyzer getBloomAnalyzer();
  }

  private static final WeakHashMap<IndexReader.CacheKey, Map<String, Analyzer>>
      BLOOM_ANALYZER_CACHE = new WeakHashMap<>();

  /**
   * This method should be used to retrieve a bloom analyzer that is compatible with the analyzer
   * used to build ngram data for the specified source field and the segment corresponding to the
   * specified LeafReader.
   */
  public static Analyzer getBloomAnalyzer(LeafReader r, String field) {
    IndexReader.CacheHelper cch = r.getCoreCacheHelper();
    IndexReader.CacheKey key = cch == null ? null : cch.getKey();
    Function<? super String, ? extends Analyzer> func =
        (f) -> {
          FieldInfo fi = r.getFieldInfos().fieldInfo(f);
          if (fi == null) {
            return null;
          }
          String postingsFormat = fi.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY);
          if (postingsFormat == null) {
            return null;
          }
          PostingsFormat pf = PostingsFormat.forName(postingsFormat);
          if (pf instanceof BloomAnalyzerSupplier) {
            return ((BloomAnalyzerSupplier) pf).getBloomAnalyzer();
          }
          return null;
        };
    if (key == null) {
      return func.apply(field);
    } else {
      return BLOOM_ANALYZER_CACHE
          .computeIfAbsent(key, (k) -> new HashMap<>())
          .computeIfAbsent(field, func);
    }
  }

  // TODO: ensure that caching on SegmentInfo key will not result in a leak. I think this should be
  //  ok, relying on cleanup from `CacheKeyWeakRef`, but we should verify.
  private static final Map<SegmentInfoWeakRef, CacheKeyEntry> CACHE_KEY_LOOKUP =
      new ConcurrentHashMap<>();
  private static final Map<IndexReader.CacheKey, WeakReference<MaxNgramAutomatonFetcher>>
      COMPUTE_MAP = new ConcurrentHashMap<>();
  private static final ReferenceQueue<SegmentInfo> CLEANUP_CACHE_KEY = new ReferenceQueue<>();

  private static final class SegmentInfoWeakRef extends WeakReference<SegmentInfo> {
    private final int hashCode;
    private final IndexReader.CacheKey cacheKey;

    public SegmentInfoWeakRef(
        SegmentInfo referent,
        IndexReader.CacheKey cacheKey,
        ReferenceQueue<? super SegmentInfo> q) {
      super(referent, q);
      this.hashCode = referent.hashCode();
      this.cacheKey = cacheKey;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else {
        return Objects.equals(get(), ((SegmentInfoWeakRef) obj).get());
      }
    }
  }

  private static final class CacheKeyEntry {
    private final IndexReader.CacheKey cacheKey;
    private final SegmentInfoWeakRef segmentKey;

    public CacheKeyEntry(IndexReader.CacheKey cacheKey, SegmentInfoWeakRef segmentKey) {
      this.cacheKey = cacheKey;
      this.segmentKey = segmentKey;
    }
  }

  public interface MaxNgramAutomatonFetcher {
    CompiledAutomaton getCompiledAutomaton(
        IndexReader.CacheKey segKey, String field, IOFunction<Void, CompiledAutomaton> compute)
        throws IOException;
  }

  public static void registerMaxNgramAutomatonFetcher(
      LeafReader r, MaxNgramAutomatonFetcher compute) {
    IndexReader.CacheHelper outerCacheHelper = r.getCoreCacheHelper();
    if (outerCacheHelper == null) {
      return;
    }
    r = FilterLeafReader.unwrap(r);
    IndexReader.CacheHelper cch;
    if (!(r instanceof SegmentReader) || (cch = r.getCoreCacheHelper()) != outerCacheHelper) {
      return;
    }
    SegmentInfoWeakRef stale;
    while ((stale = (SegmentInfoWeakRef) CLEANUP_CACHE_KEY.poll()) != null) {
      CacheKeyEntry removed = CACHE_KEY_LOOKUP.remove(stale);
      COMPUTE_MAP.remove(stale.cacheKey);
      assert stale == removed.segmentKey;
    }
    IndexReader.CacheKey cacheKey = cch.getKey();
    SegmentInfoWeakRef si =
        new SegmentInfoWeakRef(
            ((SegmentReader) r).getSegmentInfo().info, cacheKey, CLEANUP_CACHE_KEY);
    boolean[] weComputed = new boolean[1];
    CacheKeyEntry ref =
        CACHE_KEY_LOOKUP.computeIfAbsent(
            si,
            (k) -> {
              weComputed[0] = true;
              return new CacheKeyEntry(cacheKey, k);
            });
    assert weComputed[0] || ref.cacheKey == cacheKey;
    COMPUTE_MAP.put(cacheKey, new WeakReference<>(compute)); // replace if present
  }

  public static CompiledAutomaton compute(
      SegmentInfo si, String field, IOFunction<Void, CompiledAutomaton> compute)
      throws IOException {
    SegmentInfoWeakRef siRef = new SegmentInfoWeakRef(si, null, null);
    CacheKeyEntry ref = CACHE_KEY_LOOKUP.get(siRef);
    IndexReader.CacheKey key;
    if (ref == null || (key = ref.cacheKey) == null) {
      return compute.apply(null);
    }
    CompiledAutomaton[] ret = new CompiledAutomaton[1];
    IOException[] computeException = new IOException[1];
    WeakReference<MaxNgramAutomatonFetcher> fetcher =
        COMPUTE_MAP.computeIfPresent(
            key,
            (k, v) -> {
              try {
                MaxNgramAutomatonFetcher f = v.get();
                if (f == null) {
                  return null;
                }
                ret[0] = f.getCompiledAutomaton(key, field, compute);
              } catch (IOException ex) {
                computeException[0] = ex;
              }
              return v;
            });
    if (fetcher == null) {
      return compute.apply(null);
    } else if (computeException[0] != null) {
      throw computeException[0];
    } else {
      return ret[0];
    }
  }


  private static final Map<Directory, String> DIRECTORY_COLLECTION_LOOKUP =
          new WeakHashMap<>();

  private static volatile MaxSubstringEnabledMonitor maxSubstringEnabledMonitor;
  public static void registerMaxSubstringEnabledLookup(SolrCore core, Directory directory) {
    if (core.getCoreContainer().isZooKeeperAware() && maxSubstringEnabledMonitor == null) {
      synchronized(BloomUtils.class) {
        if (maxSubstringEnabledMonitor == null) {
          maxSubstringEnabledMonitor = new MaxSubstringEnabledMonitor(core.getCoreContainer().getZkController().getZkClient());
        }
      }
    }

    DIRECTORY_COLLECTION_LOOKUP.put(FilterDirectory.unwrap(directory), core.getCoreDescriptor().getCollectionName());
  }
  public static boolean isMaxSubstringWriteEnabled(Directory directory) {
    String collection = DIRECTORY_COLLECTION_LOOKUP.get(FilterDirectory.unwrap(directory));
    return collection != null && maxSubstringEnabledMonitor != null && maxSubstringEnabledMonitor.isEnabled(collection);
  }

  private static class MaxSubstringEnabledMonitor {
    private final ZkDynamicProperty maxSubstringEnabledProperty;
    private final ZkDynamicProperty maxSubstringBlacklistProperty;
    private final ZkDynamicProperty maxSubstringWhitelistProperty;
    private volatile List<String> whitelist;
    private volatile List<String> blacklist;
    private volatile boolean enabled;
    private MaxSubstringEnabledMonitor(SolrZkClient zkClient) {
      maxSubstringEnabledProperty = new ZkDynamicProperty(zkClient, "max_substring/enabled", zkValue -> {
        enabled = zkValue != null ? Boolean.valueOf(new String(zkValue)) : true;
      });
      maxSubstringBlacklistProperty = new ZkDynamicProperty(zkClient, "max_substring/blacklist", zkValue -> {
        blacklist = zkValue != null && zkValue.length > 0 ? Arrays.asList(new String(zkValue).split(",")) : Collections.emptyList();
      });
      maxSubstringWhitelistProperty = new ZkDynamicProperty(zkClient, "max_substring/whitelist", zkValue -> {
        whitelist = zkValue != null && zkValue.length > 0 ? Arrays.asList(new String(zkValue).split(",")) : Collections.emptyList();
      });
    }

    private boolean isEnabled(String collection) {
      if (enabled) {
        return !blacklist.contains(collection);
      } else {
        return whitelist.contains(collection);
      }
    }
  }
}
