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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testInitialization() {
    SolrCore core = h.getCore();

    // Build an explicit list
    // -----------------------------------------------
    List<String> names0 = new ArrayList<>();
    names0.add(MoreLikeThisComponent.COMPONENT_NAME);

    NamedList<List<String>> args = new NamedList<>();
    args.add(SearchHandler.INIT_COMPONENTS, names0);
    try (SearchHandler handler = new SearchHandler()) {
      handler.init(args);
      handler.inform(core);

      assertEquals(1, handler.getComponents().size());
      assertEquals(
          core.getSearchComponent(MoreLikeThisComponent.COMPONENT_NAME),
          handler.getComponents().get(0));
    } catch (IOException e) {
      fail("IOException closing SearchHandler");
    }

    // Build an explicit list that includes the debug comp.
    // -----------------------------------------------
    names0 = new ArrayList<>();
    names0.add(FacetComponent.COMPONENT_NAME);
    names0.add(DebugComponent.COMPONENT_NAME);
    names0.add(MoreLikeThisComponent.COMPONENT_NAME);

    args = new NamedList<>();
    args.add(SearchHandler.INIT_COMPONENTS, names0);
    try (SearchHandler handler = new SearchHandler()) {
      handler.init(args);
      handler.inform(core);

      assertEquals(3, handler.getComponents().size());
      assertEquals(
          core.getSearchComponent(FacetComponent.COMPONENT_NAME), handler.getComponents().get(0));
      assertEquals(
          core.getSearchComponent(DebugComponent.COMPONENT_NAME), handler.getComponents().get(1));
      assertEquals(
          core.getSearchComponent(MoreLikeThisComponent.COMPONENT_NAME),
          handler.getComponents().get(2));
    } catch (IOException e) {
      fail("Exception when closing SearchHandler");
    }

    // First/Last list
    // -----------------------------------------------
    names0 = new ArrayList<>();
    names0.add(MoreLikeThisComponent.COMPONENT_NAME);

    List<String> names1 = new ArrayList<>();
    names1.add(FacetComponent.COMPONENT_NAME);

    args = new NamedList<>();
    args.add(SearchHandler.INIT_FIRST_COMPONENTS, names0);
    args.add(SearchHandler.INIT_LAST_COMPONENTS, names1);
    try (SearchHandler handler = new SearchHandler()) {
      handler.init(args);
      handler.inform(core);

      List<SearchComponent> comps = handler.getComponents();
      assertEquals(2 + handler.getDefaultComponents().size(), comps.size());
      assertEquals(core.getSearchComponent(MoreLikeThisComponent.COMPONENT_NAME), comps.get(0));
      assertEquals(
          core.getSearchComponent(FacetComponent.COMPONENT_NAME), comps.get(comps.size() - 2));
      // Debug component is always last in this case
      assertEquals(
          core.getSearchComponent(DebugComponent.COMPONENT_NAME), comps.get(comps.size() - 1));
    } catch (IOException e) {
      fail("Exception when closing SearchHandler");
    }
  }

  @Test
  public void testZkConnected() throws Exception {
    MiniSolrCloudCluster miniCluster =
        new MiniSolrCloudCluster(5, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(5, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testSolrCloudCollection";
      String configName = "solrCloudCollectionConfig";
      miniCluster.uploadConfigSet(
          SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.createCollection(collectionName, configName, 2, 2)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .process(miniCluster.getSolrClient());

      QueryRequest req = new QueryRequest();
      QueryResponse rsp = req.process(cloudSolrClient, collectionName);
      assertTrue(rsp.getResponseHeader().getBooleanArg("zkConnected"));

      Collection<Slice> slices =
          cloudSolrClient.getClusterState().getCollection(collectionName).getSlices();
      Slice slice = getRandomEntry(slices);
      Replica replica = getRandomEntry(slice.getReplicas());
      JettySolrRunner jetty = miniCluster.getReplicaJetty(replica);
      // Use the replica's core URL to avoid ZK communication
      try (SolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl()).build()) {
        jetty.getCoreContainer().getZkController().getZkClient().close();
        rsp = req.process(client);
        assertFalse(rsp.getResponseHeader().getBooleanArg("zkConnected"));
      }
    } finally {
      miniCluster.shutdown();
    }
  }

  @Test
  public void testRequireZkConnected() throws Exception {
    MiniSolrCloudCluster miniCluster =
        new MiniSolrCloudCluster(5, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(5, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testRequireZkConnectedCollection";
      String configName = collectionName + "Config";
      miniCluster.uploadConfigSet(
          SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.createCollection(collectionName, configName, 2, 2)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .process(miniCluster.getSolrClient());

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(ShardParams.SHARDS_TOLERANT, "requireZkConnected");
      QueryRequest req = new QueryRequest(params);
      QueryResponse rsp = req.process(cloudSolrClient, collectionName);
      assertTrue(rsp.getResponseHeader().getBooleanArg("zkConnected"));

      Collection<Slice> slices =
          cloudSolrClient.getClusterState().getCollection(collectionName).getSlices();
      Slice disconnectedSlice = getRandomEntry(slices);
      Replica disconnectedReplica = getRandomEntry(disconnectedSlice.getReplicas());
      JettySolrRunner disconnectedJetty = miniCluster.getReplicaJetty(disconnectedReplica);
      // Use the replica's core URL to avoid ZK communication
      try (SolrClient solrClient =
          new HttpSolrClient.Builder(disconnectedReplica.getCoreUrl()).build()) {
        ignoreException("ZooKeeper is not connected");
        disconnectedJetty.getCoreContainer().getZkController().getZkClient().close();
        req.process(solrClient);
        fail(
            "An exception should be thrown when ZooKeeper is not connected and shards.tolerant=requireZkConnected");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("ZooKeeper is not connected"));
      }
    } finally {
      miniCluster.shutdown();
    }
  }

  @Test
  public void testRequireZkConnectedDistrib() throws Exception {
    MiniSolrCloudCluster miniCluster =
        new MiniSolrCloudCluster(2, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(2, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testRequireZkConnectedDistribCollection";
      String configName = collectionName + "Config";
      miniCluster.uploadConfigSet(
          SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.createCollection(collectionName, configName, 2, 1)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .process(miniCluster.getSolrClient());

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(ShardParams.SHARDS_TOLERANT, "requireZkConnected");
      QueryRequest req = new QueryRequest(params);
      QueryResponse rsp = req.process(cloudSolrClient, collectionName);
      assertTrue(rsp.getResponseHeader().getBooleanArg("zkConnected"));

      Collection<Slice> slices =
          cloudSolrClient.getClusterState().getCollection(collectionName).getSlices();
      Slice disconnectedSlice = getRandomEntry(slices);
      Replica disconnectedReplica = getRandomEntry(disconnectedSlice.getReplicas());

      // Query a coordinating replica that is connected to ZooKeeper
      Slice connectedSlice = getRandomEntry(slices);
      while (connectedSlice.getName().equals(disconnectedSlice.getName())) {
        connectedSlice = getRandomEntry(slices);
      }
      Replica connectedReplica = connectedSlice.getReplicas().iterator().next();
      try (SolrClient solrClient =
          new HttpSolrClient.Builder(connectedReplica.getCoreUrl()).build()) {
        ignoreException("ZooKeeper is not connected");
        ignoreException("no active servers hosting shard:");
        JettySolrRunner disconnectedJetty = miniCluster.getReplicaJetty(disconnectedReplica);
        disconnectedJetty.getCoreContainer().getZkController().getZkClient().close();
        req.process(solrClient);
        fail(
            "An exception should be thrown when ZooKeeper is not connected and shards.tolerant=requireZkConnected");
      } catch (Exception e) {
        assertTrue(
            "Unrecognized exception message: " + e,
            e.getMessage().contains("no active servers hosting shard:")
                || e.getMessage().contains("ZooKeeper is not connected"));
      }
    } finally {
      miniCluster.shutdown();
      unIgnoreException("no active servers hosting shard:");
      unIgnoreException("ZooKeeper is not connected");
    }
  }

  @Test
  public void testLuceneIOExceptionHandling() throws Exception {
    System.setProperty("solr.max.booleanClauses", String.valueOf(1));
    MiniSolrCloudCluster miniCluster =
        new MiniSolrCloudCluster(1, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(1, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testLuceneIOExceptionHandling";
      String configName = collectionName + "Config";
      miniCluster.uploadConfigSet(
          SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.createCollection(collectionName, configName, 2, 1)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .process(miniCluster.getSolrClient());

      for (int i = 0; i < 5; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("title", "test" + i);
        cloudSolrClient.add(collectionName, doc);
      }
      cloudSolrClient.commit(collectionName);

      Collection<BytesRef> terms = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        terms.add(newBytesRef("term" + i));
      }
      TermInSetQuery termInSetQuery = new TermInSetQuery("name", terms);
      SolrQuery solrQuery = new SolrQuery(termInSetQuery.toString());
      final QueryRequest req = new QueryRequest(solrQuery);
      req.setMethod(SolrRequest.METHOD.POST);
      BaseHttpSolrClient.RemoteSolrException e =
          assertThrows(
              BaseHttpSolrClient.RemoteSolrException.class,
              () -> req.process(cloudSolrClient, collectionName));
      assertEquals(400, e.code());
      assertTrue(e.getMessage().contains("org.apache.solr.search.SyntaxError"));

      solrQuery = new SolrQuery("{!surround maxBasicQueries=1 df=title}99W(test1,test2,test3)");
      final QueryRequest req2 = new QueryRequest(solrQuery);
      req2.setMethod(SolrRequest.METHOD.POST);
      e =
          assertThrows(
              BaseHttpSolrClient.RemoteSolrException.class,
              () -> req2.process(cloudSolrClient, collectionName));
      assertEquals(400, e.code());
      assertTrue(e.getMessage().contains("Exceeded maximum of 1 basic queries."));

      solrQuery = new SolrQuery("{!surround maxBasicQueries=1000 df=title}10W(tes*,test*)");
      final QueryRequest req3 = new QueryRequest(solrQuery);
      req3.setMethod(SolrRequest.METHOD.POST);
      e =
          assertThrows(
              BaseHttpSolrClient.RemoteSolrException.class,
              () -> req3.process(cloudSolrClient, collectionName));
      assertEquals(400, e.code());
      assertTrue(
          e.getMessage()
              .contains("Query contains too many nested clauses; maxClauseCount is set to 1"));
    } finally {
      System.clearProperty("solr.max.booleanClauses");
      miniCluster.shutdown();
    }
  }

  private static <T> T getRandomEntry(Collection<T> collection) {
    if (null == collection || collection.isEmpty()) return null;

    Iterator<T> iterator = collection.iterator();
    T entry = iterator.next();
    int index = 0, rand = random().nextInt(collection.size());
    while (index++ < rand) entry = iterator.next();
    return entry;
  }
}
