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
package org.apache.solr.client.solrj.io.stream;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountDistinctMetric;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.PercentileMetric;
import org.apache.solr.client.solrj.io.stream.metrics.StdMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.SolrDefaultStreamFactory;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
@ThreadLeakLingering(linger = 0)
public class StreamIdleTimeoutTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String TEST_COLLECTION = "collection1";
  private static final String id = "id";
  private static final int SHARD_COUNT = 2; //change to 1 then the test case will succeed

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig(
            "conf",
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .withJettyConfig(jetty -> jetty.withConnectorIdleTimeout(5000L)).configure();

    String collection = TEST_COLLECTION;
    CollectionAdminRequest.createCollection(collection, "conf", SHARD_COUNT, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, SHARD_COUNT, SHARD_COUNT);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), TEST_COLLECTION);
  }

  @Test
  public void testCloudSolrStream() throws Exception {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      log.info("Jetty Base URL: "+ jettySolrRunner.getBaseUrl());
    }

    UpdateRequest updateRequest = new UpdateRequest();
    StringBuilder longStrBuilder = new StringBuilder();
    for (int i = 0 ; i < 1000; i ++) {
      longStrBuilder.append('a');
    }

    //need a longer value so the flush on the server would be in pending state (cannot flush everything in one go).
    //this is necessary to trigger the failure handling on IdleTimeout
    String longPrefix = longStrBuilder.toString();
    for (int i = 0 ; i < 10000; i ++) {
      updateRequest.add(id, "c!" + i, "a_s", longPrefix + i); //this will goto shard 1 (with shard count as 2)
    }
    updateRequest.commit(cluster.getSolrClient(), TEST_COLLECTION);
    for (int i = 0 ; i < 10000; i ++) {
      updateRequest.add(id, "a!" + i, "a_s", longPrefix + i); //this will goto shard 2 (with shard count as 2)
    }
    updateRequest.commit(cluster.getSolrClient(), TEST_COLLECTION);
    log.info("Committed all updates. Now start streaming");

    SolrDefaultStreamFactory factory = new SolrDefaultStreamFactory();

    factory.withCollectionZkHost(TEST_COLLECTION, cluster.getZkServer().getZkAddress());

    CloudSolrStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    StreamExpression expression =
        StreamExpressionParser.parse(
            "search("
                + TEST_COLLECTION
                + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"id asc\", qt=\"/export\")");
    stream = new CloudSolrStream(expression, factory);
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    assertEquals(20000, tuples.size());
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException, InterruptedException {
    List<Tuple> tuples = new ArrayList<>();

    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
        if (tuples.size() % 1000 == 0) {
          TimeUnit.SECONDS.sleep(1); //put a small pause to emulate some processing
          log.info("Processed {} out of 20000 tuples", tuples.size());
        }
      }
    }
    return tuples;
  }
}
