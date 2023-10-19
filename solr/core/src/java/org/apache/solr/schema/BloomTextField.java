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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.BloomStrField.BloomAnalyzerSupplier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A specialized TextField variant that facilitates configuration of a ngram subfield (populated at
 * segment flush by a custom PostingsFormat) that can be used to pre-filter terms that must be
 * evaluated for substring/wildcard/regex search.
 */
public final class BloomTextField extends StrField implements SchemaAware {

  @Override
  public boolean isPolyField() {
    return true;
  }

  private IndexSchema schema;
  private FieldType bloomFieldType;
  private FieldType maxStringFieldType;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    this.schema = schema;
    PostingsFormat pf = BloomStrField.getPostingsFormat(args);
    bloomFieldType = BloomStrField.getFieldType(schema, (PostingsFormat & BloomAnalyzerSupplier) pf);
    String maxSubstring = args.remove("maxSubstring");
    if (maxSubstring == null || "true".equals(maxSubstring)) {
      maxStringFieldType = BloomStrField.getMaxSubstringFieldType(schema, pf);
    }
    super.init(schema, args);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    List<IndexableField> ret = new ArrayList<>(3);
    String bloomFieldName =
        field
            .getName()
            .concat(field.multiValued() ? BloomStrField.BLOOM_FIELD_BASE_SUFFIX_MULTI : BloomStrField.BLOOM_FIELD_BASE_SUFFIX_SINGLE);

    // reserve a spot in fieldInfos, so that our PostingsFormat sees the subfield
    ret.add(createField(bloomFieldName, "", schema.getField(bloomFieldName)));

    if (maxStringFieldType != null) {
      // hack companion field of max substring too
      String maxSubstringFieldName = field.getName().concat("_max_substring");
      ret.add(createField(maxSubstringFieldName, "", schema.getField(maxSubstringFieldName)));
    }

    ret.addAll(super.createFields(field, value));
    return ret;
  }

  @Override
  public void inform(IndexSchema schema) {
    BloomStrField.registerDynamicSubfields(schema, bloomFieldType, maxStringFieldType);
  }
}
