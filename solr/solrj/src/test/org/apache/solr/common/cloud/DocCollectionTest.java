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
package org.apache.solr.common.cloud;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.ConfigSetsHandler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DocCollectionTest extends SolrTestCase {

  public void testCopyWith() {
    DocCollection collection = createDocCollection();
    assertEquals(Replica.State.DOWN, collection.getSlice("shard1").getReplica("node1").getState());

    PerReplicaStates newReplicaStates = new PerReplicaStates(
                    "state.json", 1, List.of("node1:2:A"));
    collection = collection.copyWith(newReplicaStates);
    assertEquals(Replica.State.ACTIVE, collection.getSlice("shard1").getReplica("node1").getState());
  }

  private DocCollection createDocCollection() {
    Set<String> liveNodes = new HashSet<>();
    liveNodes.add("node1");

    Map<String, Slice> slices = new HashMap<>();
    Map<String, Replica> sliceToProps = new HashMap<>();
    Map<String, Object> props = new HashMap<>();
    String nodeName = "127.0.0.1:10000_solr";
    props.put(ZkStateReader.NODE_NAME_PROP, nodeName);
    props.put(ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(nodeName, "http"));
    props.put(ZkStateReader.CORE_NAME_PROP, "core1");
    props.put(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME);
    props.put(Replica.ReplicaStateProps.STATE, Replica.State.DOWN.toString()); //start with down

    Replica replica = new Replica("node1", props, "collection1", "shard1");
    sliceToProps.put("node1", replica);
    Slice slice = new Slice("shard1", sliceToProps, null, "collection1");
    slices.put("shard1", slice);
    return new DocCollection("collection1", slices, props, DocRouter.DEFAULT);
  }


}
