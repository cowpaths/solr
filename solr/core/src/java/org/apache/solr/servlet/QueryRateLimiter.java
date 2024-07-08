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

import static org.apache.solr.core.RateLimiterConfig.RL_CONFIG_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of RequestRateLimiter specific to query request types. Most of the actual work is
 * delegated to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
  private static QueryRateLimiter DEFAULT =
      new QueryRateLimiter(new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY), null);

  public QueryRateLimiter(RateLimiterConfig config, byte[] configData) {
    super(config);
    this.configData = configData;
  }

  public static RateLimiterConfig processConfigChange(
      SolrRequest.SolrRequestType requestType,
      RateLimiterConfig rateLimiterConfig,
      Map<String, Object> properties)
      throws IOException {
    byte[] configInput = Utils.toJSON(properties.get(RL_CONFIG_KEY));

    RateLimiterPayload rateLimiterMeta;
    if (configInput == null || configInput.length == 0) {
      rateLimiterMeta = null;
    } else {
      rateLimiterMeta = mapper.readValue(configInput, RateLimiterPayload.class);
    }

    if (rateLimiterConfig == null || rateLimiterConfig.shouldUpdate(rateLimiterMeta)) {
      // no prior config, or config has changed; return the new config
      return new RateLimiterConfig(requestType, rateLimiterMeta);
    } else {
      return null;
    }
  }

  // To be used in initialization
  @SuppressWarnings({"unchecked"})
  public static Map<SolrRequest.SolrRequestType, QueryRateLimiter> read(SolrZkClient zkClient) {
    try {

      if (zkClient == null) {
        return Map.of(SolrRequest.SolrRequestType.QUERY, DEFAULT);
      }
      Object obj =
          Utils.fromJSON(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      if (obj == null) return Map.of(SolrRequest.SolrRequestType.QUERY, DEFAULT);
      else {
        return parseRateLimiterConfig((Map<String, Object>) obj);
      }
    } catch (KeeperException.NoNodeException e) {
      return Map.of(SolrRequest.SolrRequestType.QUERY, DEFAULT);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(
          "Error reading cluster property", SolrZkClient.checkInterrupted(e));
    } catch (IOException e) {
      throw new RuntimeException("Encountered an IOException " + e.getMessage());
    }
  }

  public static Map<SolrRequest.SolrRequestType, QueryRateLimiter> parseRateLimiterConfig(
      Map<String, Object> cfg) throws IOException {
    Object obj = cfg.get(RL_CONFIG_KEY);
    if (obj == null) return Map.of(SolrRequest.SolrRequestType.QUERY, DEFAULT);
    Map<SolrRequest.SolrRequestType, QueryRateLimiter> result = new HashMap<>();
    List<Map<String, Object>> list = null;
    if (obj instanceof List) {
      list = (List<Map<String, Object>>) obj;
    } else {
      list = List.of((Map<String, Object>) obj);
    }
    if (list.isEmpty()) return Map.of(SolrRequest.SolrRequestType.QUERY, DEFAULT);
    for (Map<String, Object> rl : list) {
      byte[] cfgData = Utils.toJSON(rl);
      RateLimiterPayload rateLimiterPayload = mapper.readValue(cfgData, RateLimiterPayload.class);
      QueryRateLimiter qrl =
          new QueryRateLimiter(
              new RateLimiterConfig(
                  SolrRequest.SolrRequestType.parse(rateLimiterPayload.type), rateLimiterPayload),
              cfgData);
      result.put(qrl.getRateLimiterConfig().requestType, qrl);
    }
    return result;
  }
}
