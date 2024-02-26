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

package org.apache.solr.servlet;

import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.api.CoordinatorV2HttpSolrCall;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.ConfigSet;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.DelegatingSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorHttpSolrCall extends HttpSolrCall {
  public static final String SYNTHETIC_COLL_PREFIX =
      Assign.SYSTEM_COLL_PREFIX + "COORDINATOR-COLL-";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String collectionName;
  private final Factory factory;

  public CoordinatorHttpSolrCall(
      Factory factory,
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cores,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    super(solrDispatchFilter, cores, request, response, retry);
    this.factory = factory;
  }

  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    this.collectionName = collectionName;
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    if (!path.endsWith("/select")) return null;
    return getCore(factory, this, collectionName, isPreferLeader);
  }

  public static SolrCore getCore(
      Factory factory, HttpSolrCall solrCall, String collectionName, boolean isPreferLeader) {
    String syntheticCoreName = factory.collectionVsCoreNameMapping.get(collectionName);
    if (syntheticCoreName != null) {
      return solrCall.cores.getCore(syntheticCoreName);
    } else {
      ZkStateReader zkStateReader = solrCall.cores.getZkController().getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollectionOrNull(collectionName, true);

      synchronized (CoordinatorHttpSolrCall.class) {
        String confName = coll.getConfigName();
        String syntheticCollectionName = getSyntheticCollectionName(confName);

        CoreContainer coreContainer = solrCall.cores;
        Map<String, String> coreProps = new HashMap<>();
        coreProps.put(CoreAdminParams.CORE_NODE_NAME, coreContainer.getHostName());
        coreProps.put(CoreAdminParams.COLLECTION, syntheticCollectionName);

        CoreDescriptor syntheticCoreDescriptor =
            new CoreDescriptor(
                collectionName,
                Paths.get(coreContainer.getSolrHome() + "/" + collectionName),
                coreProps,
                coreContainer.getContainerProperties(),
                coreContainer.getZkController());

        ConfigSet coreConfig =
            coreContainer.getConfigSetService().loadConfigSet(syntheticCoreDescriptor, confName);
        syntheticCoreDescriptor.setConfigSetTrusted(coreConfig.isTrusted());
        SolrCore syntheticCore = new SolrCore(coreContainer, syntheticCoreDescriptor, coreConfig);

        coreContainer.registerCore(syntheticCoreDescriptor, syntheticCore, false, false);

        // after this point the sync core should be available in the container. Double check
        if (coreContainer.getCore(syntheticCore.getName()) != null) {
          factory.collectionVsCoreNameMapping.put(collectionName, syntheticCore.getName());

          // for the watcher, only remove on collection deletion (ie collection == null), since
          // watch from coordinator is collection specific
          solrCall
              .cores
              .getZkController()
              .getZkStateReader()
              .registerDocCollectionWatcher(
                  collectionName,
                  collection -> {
                    if (collection == null) {
                      factory.collectionVsCoreNameMapping.remove(collectionName);
                      return true;
                    } else {
                      return false;
                    }
                  });
          if (log.isDebugEnabled()) {
            log.debug("coordinator node, returns synthetic core: {}", syntheticCore.getName());
          }
          return syntheticCore;
        }
      }
      return null;
    }
  }

  public static String getSyntheticCollectionName(String configName) {
    return SYNTHETIC_COLL_PREFIX + configName;
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = wrappedReq(solrReq, collectionName, this);
    }
  }

  public static SolrQueryRequest wrappedReq(
      SolrQueryRequest delegate, String collectionName, HttpSolrCall httpSolrCall) {
    Properties p = new Properties();
    if (collectionName != null) {
      p.put(CoreDescriptor.CORE_COLLECTION, collectionName);
    }
    p.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.PULL.toString());
    p.put(CoreDescriptor.CORE_SHARD, "_");

    CloudDescriptor cloudDescriptor =
        new CloudDescriptor(
            delegate.getCore().getCoreDescriptor(), delegate.getCore().getName(), p);
    return new DelegatingSolrQueryRequest(delegate) {
      @Override
      public HttpSolrCall getHttpSolrCall() {
        return httpSolrCall;
      }

      @Override
      public CloudDescriptor getCloudDescriptor() {
        return cloudDescriptor;
      }
    };
  }

  // The factory that creates an instance of HttpSolrCall
  public static class Factory implements SolrDispatchFilter.HttpSolrCallFactory {
    private final Map<String, String> collectionVsCoreNameMapping = new ConcurrentHashMap<>();

    @Override
    public HttpSolrCall createInstance(
        SolrDispatchFilter filter,
        String path,
        CoreContainer cores,
        HttpServletRequest request,
        HttpServletResponse response,
        boolean retry) {
      if ((path.startsWith("/____v2/") || path.equals("/____v2"))) {
        return new CoordinatorV2HttpSolrCall(this, filter, cores, request, response, retry);
      } else if (path.startsWith("/" + SYNTHETIC_COLL_PREFIX)) {
        return SolrDispatchFilter.HttpSolrCallFactory.super.createInstance(
            filter, path, cores, request, response, retry);
      } else {
        return new CoordinatorHttpSolrCall(this, filter, cores, request, response, retry);
      }
    }
  }
}
