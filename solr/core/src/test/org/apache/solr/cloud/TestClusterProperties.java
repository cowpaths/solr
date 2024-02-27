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

package org.apache.solr.cloud;

import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterProperties extends SolrCloudTestCase {

  private ClusterProperties props;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).configure();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    props = new ClusterProperties(zkClient());
  }

  @Test
  public void testSetPluginClusterProperty() throws Exception {
    String propertyName = ClusterProperties.EXT_PROPRTTY_PREFIX + "pluginA.propertyA";
    CollectionAdminRequest.setClusterProperty(propertyName, "valueA")
        .process(cluster.getSolrClient());
    assertEquals("valueA", props.getClusterProperty(propertyName, null));
  }

  @Test(expected = SolrException.class)
  public void testSetInvalidPluginClusterProperty() throws Exception {
    String propertyName = "pluginA.propertyA";
    CollectionAdminRequest.setClusterProperty(propertyName, "valueA")
        .process(cluster.getSolrClient());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWatchedNodeProperties() {
    WatchedClusterProperties wcp = new WatchedClusterProperties("node1");
    NamedList<String> listener1 = new NamedList<>();
    NamedList<String> all = new NamedList<>();
    NamedList<String> listener3 = new NamedList<>();
    wcp.watchNodeProperty("p1", (key, value) -> listener1.add(key, value));
    wcp.watchNodeProperty(null, (key, value) -> all.add(key, value));
    BiConsumer<String, String> p3Listener = (key, value) -> listener3.add(key, value);
    wcp.watchNodeProperty("p3", p3Listener);
    wcp.onChange(
        (Map<String, Object>)
            Utils.fromJSONString(
                "{\n"
                    + "  \"watched-node-properties\" :{\n"
                    + "    \"node1\" : {\n"
                    + "      \"p1\" : \"v1\",\n"
                    + "      \"p2\" : \"v2\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"));

    assertEquals(1, listener1.size());
    assertEquals("v1", listener1.get("p1"));
    assertEquals(2, all.size());
    assertEquals("v1", all.get("p1"));
    assertEquals("v2", all.get("p2"));
    assertEquals(0, listener3.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWatchedClusterProperties() {
    WatchedClusterProperties wcp = new WatchedClusterProperties("node1");
    NamedList<String> listener1 = new NamedList<>();
    NamedList<String> all = new NamedList<>();
    NamedList<String> listener3 = new NamedList<>();
    wcp.watchProperty("p1", (key, value) -> listener1.add(key, value));
    wcp.watchProperty(null, (key, value) -> all.add(key, value));
    BiConsumer<String, String> p3Listener = (key, value) -> listener3.add(key, value);
    wcp.watchProperty("p3", p3Listener);

    wcp.onChange(
        (Map<String, Object>)
            Utils.fromJSONString(
                "{\n"
                    + "  \"watched-properties\": {\n"
                    + "    \"p1\": \"v1\",\n"
                    + "    \"p2\": \"v2\"\n"
                    + "  }\n"
                    + "}"));
    assertEquals(1, listener1.size());
    assertEquals("v1", listener1.get("p1"));
    assertEquals(2, all.size());
    assertEquals("v1", all.get("p1"));
    assertEquals("v2", all.get("p2"));
    assertEquals(0, listener3.size());
    listener1.clear();
    all.clear();
    listener3.clear();
    wcp.onChange(
        (Map<String, Object>)
            Utils.fromJSONString(
                "{\n"
                    + "  \"watched-properties\": {\n"
                    + "    \"p1\": \"v1\",\n"
                    + "    \"p2\": \"v2\"\n"
                    + "  }\n"
                    + "}"));
    assertEquals(0, listener1.size());
    assertEquals(0, all.size());
    assertEquals(0, listener3.size());
    wcp.onChange(
        (Map<String, Object>)
            Utils.fromJSONString(
                "{\n" + "  \"watched-properties\": {\n" + "    \"p3\": \"v3\"\n" + "  }\n" + "}"));
    assertEquals(1, listener1.size());
    assertEquals(null, listener1.get("p1"));
    assertEquals(3, all.size());
    assertEquals(1, listener3.size());
    assertEquals("v3", listener3.get("p3"));
  }
}
