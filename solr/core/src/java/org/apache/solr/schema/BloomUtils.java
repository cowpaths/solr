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
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
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

  /** Single-valued ngram suffix */
  private static final String BLOOM_FIELD_BASE_SUFFIX_SINGLE = BLOOM_FIELD_BASE_SUFFIX.concat("S");

  /** multi-valued ngram suffix */
  private static final String BLOOM_FIELD_BASE_SUFFIX_MULTI = BLOOM_FIELD_BASE_SUFFIX.concat("M");

  private static final int REQUIRE_LENGTH_DIFFERENTIAL = BLOOM_FIELD_BASE_SUFFIX_SINGLE.length();

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

  private static final boolean DEFAULT_ENABLE_NGRAMS = !"false".equals(System.getProperty("enableNgrams"));

  private static final int DEFAULT_DEFAULT_POSTINGS_LIMIT = 16;
  private static final int DEFAULT_POSTINGS_LIMIT;

  static {
    String spec = System.getProperty("collatedPostingsLimit");
    if (spec == null) {
      DEFAULT_POSTINGS_LIMIT = DEFAULT_DEFAULT_POSTINGS_LIMIT;
    } else {
      int setLimit;
      try {
        setLimit = Integer.parseInt(spec);
      } catch (NumberFormatException ex) {
        // TODO: log this
        setLimit = DEFAULT_DEFAULT_POSTINGS_LIMIT;
      }
      if (setLimit == 0) {
        DEFAULT_POSTINGS_LIMIT = DEFAULT_DEFAULT_POSTINGS_LIMIT;
      } else if (setLimit < 0) {
        DEFAULT_POSTINGS_LIMIT = -1;
      } else {
        DEFAULT_POSTINGS_LIMIT = setLimit;
      }
    }
  }

  private static final ThreadLocal<Boolean> ENABLE_NGRAMS =
      new ThreadLocal<>() {
        @Override
        protected Boolean initialValue() {
          return DEFAULT_ENABLE_NGRAMS;
        }
      };

  private static final ThreadLocal<Integer> COLLATED_POSTINGS_LIMIT =
      new ThreadLocal<>() {
        @Override
        protected Integer initialValue() {
          return DEFAULT_POSTINGS_LIMIT;
        }
      };

  public static void init(SolrQueryRequest req) {
    String spec = req.getParams().get("enableNgrams");
    boolean enableNgrams;
    if (spec == null) {
      enableNgrams = DEFAULT_ENABLE_NGRAMS;
    } else if ("false".equals(spec)) {
      enableNgrams = false;
    } else if ("true".equals(spec)) {
      enableNgrams = true;
    } else {
      throw new IllegalArgumentException("bad enableNgrams spec: " + spec);
    }
    ENABLE_NGRAMS.set(enableNgrams);
    int postingsLimit;
    spec = req.getParams().get("collatedPostingsLimit");
    if (spec == null) {
      postingsLimit = DEFAULT_POSTINGS_LIMIT;
    } else {
      try {
        postingsLimit = Integer.parseInt(spec);
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException("bad collatedPostingsLimit spec: " + spec);
      }
      if (postingsLimit == 0) {
        // explicitly requested default
        postingsLimit = DEFAULT_POSTINGS_LIMIT;
      } else if (postingsLimit < 0) {
        // normalize any negative values to -1 (disabled).
        postingsLimit = -1;
      }
    }
    COLLATED_POSTINGS_LIMIT.set(postingsLimit);
  }

  public static int collatedPostingsLimit() {
    return COLLATED_POSTINGS_LIMIT.get();
  }

  public static boolean enableNgrams() {
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

  static void registerDynamicSubfields(IndexSchema schema, FieldType bloomFieldType) {
    for (boolean multiValued : new boolean[] {true, false}) {
      String name =
          "*" + (multiValued ? BLOOM_FIELD_BASE_SUFFIX_MULTI : BLOOM_FIELD_BASE_SUFFIX_SINGLE);
      Map<String, String> props = new HashMap<>();
      props.put("multiValued", Boolean.toString(multiValued));
      int p = SchemaField.calcProps(name, bloomFieldType, props);
      schema.registerDynamicFields(SchemaField.create(name, bloomFieldType, p, null));
    }
  }

  public static String getNgramFieldName(SchemaField sf) {
    return sf.getName()
        .concat(
            sf.multiValued()
                ? BloomUtils.BLOOM_FIELD_BASE_SUFFIX_MULTI
                : BloomUtils.BLOOM_FIELD_BASE_SUFFIX_SINGLE);
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
}
