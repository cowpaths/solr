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
package org.apache.solr.search.facet;

import org.apache.lucene.index.*;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSetUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Facets numbers into a hash table. The number is either a raw numeric DocValues value, or a term
 * global ordinal integer. Limitations:
 *
 * <ul>
 *   <li>doesn't handle prefix, but could easily be added
 *   <li>doesn't handle mincount==0 -- you're better off with an array alg
 * </ul>
 */
class FacetFieldProcessorByHashMF extends FacetFieldProcessor {
  // must be a power of two, non-final to support setting by tests
  static int MAXIMUM_STARTING_TABLE_SIZE = 1024;
  SchemaField[] schemaFields;

  private static class FieldValuesKey {
    private final long[] array;
    private final int hashCode;

    FieldValuesKey(long[] array) {
      this.array = array;
      this.hashCode = Arrays.hashCode(array);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof FieldValuesKey) {
        return Arrays.equals(array, ((FieldValuesKey) o).array);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  /** a hash table with long keys (what we're counting) and integer values (counts) */
  private static class LongCounts {

    static final float LOAD_FACTOR = 0.7f;

    // maintain the counts here since we need them to tell if there was actually a value anyway
    long[] counts;

    int threshold;
    Map<FieldValuesKey, Integer> countsMap;
    FieldValuesKey[] multiVals;

    /** sz must be a power of two */
    LongCounts(int sz) {
      counts = new long[sz];
      threshold = (int) (sz * LOAD_FACTOR);
      countsMap = new HashMap<>(sz);
    }

    /** Current number of slots in the hash table. Also equal to the cardinality */
    int numSlots() {
      return countsMap.size();
    }

    FieldValuesKey add(long[] vals) {
      FieldValuesKey key = new FieldValuesKey(vals);
      countsMap.computeIfPresent(key, (k, v) -> v + 1);
      countsMap.putIfAbsent(key, 1);
      return key;
    }

    // break out the map counts into an array
    void copyMapToArray() {
      counts = new long[this.numSlots()];
      multiVals = new FieldValuesKey[this.numSlots()];
      int i = 0;
      for (Map.Entry<FieldValuesKey, Integer> entry : countsMap.entrySet()) {
        multiVals[i] = entry.getKey();
        counts[i] = entry.getValue();
        i++;
      }
    }
  }

  /** A hack instance of Calc for Term ordinals in DocValues. */
  // TODO consider making FacetRangeProcessor.Calc facet top level; then less of a hack?
  private class TermOrdCalc extends FacetRangeProcessor.Calc {

    IntFunction<BytesRef> lookupOrdFunction; // set in collectDocs()!
    SchemaField calcSf;

    TermOrdCalc(SchemaField calcSf) throws IOException {
      super(sf);
      this.calcSf = calcSf;
    }

    @Override
    public long bitsToSortableBits(long globalOrd) {
      return globalOrd;
    }

    /** To be returned in "buckets"/"val" */
    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long globalOrd) {
      BytesRef bytesRef = lookupOrdFunction.apply((int) globalOrd);
      // note FacetFieldProcessorByArray.findTopSlots also calls SchemaFieldType.toObject
      return calcSf.getType().toObject(calcSf, bytesRef).toString();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public String formatValue(Comparable val) {
      return (String) val;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Comparable parseStr(String rawval) throws ParseException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Comparable parseAndAddGap(Comparable value, String gap) throws ParseException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected long toSortableDocValue(Comparable calcValue) {
      return 0; // TODO
    }
  }

  FacetRangeProcessor.Calc[] calc;
  LongCounts table;

  FacetFieldProcessorByHashMF(FacetContext fcontext, FacetField freq, SchemaField[] schemaFields) {
    super(fcontext, freq, null);
    if (freq.mincount == 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, getClass() + " doesn't support mincount=0");
    }
    if (freq.prefix != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          getClass() + " doesn't support prefix"); // yet, but it could
    }
    for (SchemaField schemaField : schemaFields) {
      FieldInfo fieldInfo = fcontext.searcher.getFieldInfos().fieldInfo(schemaField.getName());
      if (fieldInfo != null
              && fieldInfo.getDocValuesType() != DocValuesType.NUMERIC
              && fieldInfo.getDocValuesType() != DocValuesType.SORTED
              && fieldInfo.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
        throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                getClass() + " only support single valued number/string with docValues");
      }
    }
    this.schemaFields = schemaFields;
  }

  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
    table = null; // gc
  }

  private SimpleOrderedMap<Object> calcFacets() throws IOException {
    calc = new FacetRangeProcessor.Calc[schemaFields.length];
    for (int i = 0; i < schemaFields.length; i++) {
      if (schemaFields[i].getType().getNumberType() != null) {
        calc[i] = FacetRangeProcessor.getNumericCalc(schemaFields[i]);
      } else {
        calc[i] = new TermOrdCalc(schemaFields[i]); // kind of a hack
      }
    }

    int possibleValues = fcontext.base.size();
    // size smaller tables so that no resize will be necessary
    int currHashSize =
        BitUtil.nextHighestPowerOfTwo((int) (possibleValues * (1 / LongCounts.LOAD_FACTOR) + 1));
    currHashSize = Math.min(currHashSize, MAXIMUM_STARTING_TABLE_SIZE);
    table = new LongCounts(currHashSize);

    // note: these methods/phases align with FacetFieldProcessorByArray's
    createCollectAcc();
    collectDocs();
    table.copyMapToArray();

    return super.findTopSlots(
        table.numSlots(), // num slots
        table.numSlots(), // cardinality
        slotNum -> { // getBucketValFromSlotNum
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < calc.length; i++) {
            sb.append(calc[i].bitsToValue(table.multiVals[slotNum].array[i]));
            if (i < calc.length - 1) {
              sb.append(", ");
            }
          }
          return sb.toString();
        },
        val -> { // getFieldQueryVal
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < calc.length; i++) {
            sb.append(calc[i].formatValue(val));
            if (i < calc.length - 1) {
              sb.append(", ");
            }
          }
          return sb.toString();
        });
  }


  private void createCollectAcc() throws IOException {
    int numSlots = table.numSlots();

    indexOrderAcc =
        new SlotAcc(fcontext) {
          @Override
          public void collect(int doc, int slot, IntFunction<SlotContext> slotContext)
                  throws IOException {}

          @Override
          public int compare(int slotA, int slotB) {
            for (int i = 0; i < schemaFields.length; i++) {
              long s1 = calc[i].bitsToSortableBits(table.multiVals[slotA].array[i]);
              long s2 = calc[i].bitsToSortableBits(table.multiVals[slotB].array[i]);
              int comparedVal = Long.compare(s1, s2);
              if (comparedVal != 0) {
                return comparedVal;
              }
            }
            return 0;
          }

          @Override
          public Object getValue(int slotNum) throws IOException {
            return null;
          }

          @Override
          public void reset() {}

          @Override
          public void resize(Resizer resizer) {}
        };

    countAcc =
        new SlotAcc.CountSlotAcc(fcontext) {
          @Override
          public void incrementCount(int slot, long count) {
            throw new UnsupportedOperationException();
          }

          @Override
          public long getCount(int slot) {
            return table.counts[slot];
          }

          @Override
          public Object getValue(int slotNum) {
            return getCount(slotNum);
          }

          @Override
          public void reset() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void collect(int doc, int slot, IntFunction<SlotContext> slotContext)
                  throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public int compare(int slotA, int slotB) {
            return Long.compare(table.counts[slotA], table.counts[slotB]);
          }

          @Override
          public void resize(Resizer resizer) {
            throw new UnsupportedOperationException();
          }
        };

    // we set the countAcc & indexAcc first so generic ones won't be created for us.
    super.createCollectAcc(fcontext.base.size(), numSlots);
  }

  private void collectDocs() throws IOException {
    for (FacetRangeProcessor.Calc currCalc : calc) {
      if (!(currCalc instanceof TermOrdCalc)) { // Numeric
        // todo: error here for now, just support strings
        throw new UnsupportedOperationException();
      }
    }
    SortedDocValues[] globalDocValues = new SortedDocValues[schemaFields.length];
    for (int i = 0; i < schemaFields.length; i++) {
      SortedDocValues currGlobalDocValues = FieldUtil.getSortedDocValues(fcontext.qcontext, schemaFields[i], null);
      globalDocValues[i] = currGlobalDocValues;
      ((TermOrdCalc) calc[i]).lookupOrdFunction =
        ord -> {
          try {
            return currGlobalDocValues.lookupOrd(ord);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    }

    DocSetUtil.collectSortedDocSet(
        fcontext.base,
        fcontext.searcher.getIndexReader(),
        new SimpleCollector() {
          final SortedDocValues[] docValues = globalDocValues; // this segment/leaf. NN
          final LongValues[] toGlobal = new LongValues[schemaFields.length]; // this segment to global ordinal. NN

          @Override
          public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
          }

          @Override
          protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
            setNextReaderFirstPhase(ctx);
            for (int i = 0; i < docValues.length; i++) {
              if (globalDocValues[i] instanceof MultiDocValues.MultiSortedDocValues) {
                MultiDocValues.MultiSortedDocValues multiDocValues =
                        (MultiDocValues.MultiSortedDocValues) globalDocValues[i];
                docValues[i] = multiDocValues.values[ctx.ord];
                toGlobal[i] = multiDocValues.mapping.getGlobalOrds(ctx.ord);
              } else {
                toGlobal[i] = LongValues.IDENTITY;
              }
            }
          }

          @Override
          public void collect(int segDoc) throws IOException {
            long[] fieldVals = new long[docValues.length];
            for (int i = 0; i < docValues.length; i++) {
              if (docValues[i].advanceExact(segDoc)) {
                fieldVals[i] = toGlobal[i].get(docValues[i].ordValue());
              }
            }
            table.add(fieldVals);
          }
        });
  }
}
