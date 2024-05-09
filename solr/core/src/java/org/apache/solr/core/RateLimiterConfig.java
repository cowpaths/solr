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

package org.apache.solr.core;

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_CONCURRENT_REQUESTS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;

public class RateLimiterConfig {
  public static final String RL_CONFIG_KEY = "rate-limiters";

  public final SolrRequest.SolrRequestType requestType;
  public boolean isEnabled;
  public long waitForSlotAcquisition;
  public int allowedRequests;
  public boolean isSlotBorrowingEnabled;
  public int guaranteedSlotsThreshold;

  /**
   * We store the config definition in order to determine whether anything has changed that would
   * call for re-initialization.
   */
  public RateLimiterPayload definition;

  public RateLimiterConfig(SolrRequest.SolrRequestType requestType) {
    this.requestType = requestType;
    update(EMPTY); // use defaults
  }

  public RateLimiterConfig(
      SolrRequest.SolrRequestType requestType,
      boolean isEnabled,
      int guaranteedSlotsThreshold,
      long waitForSlotAcquisition,
      int allowedRequests,
      boolean isSlotBorrowingEnabled) {
    this.requestType = requestType;
    this.isEnabled = isEnabled;
    this.guaranteedSlotsThreshold = guaranteedSlotsThreshold;
    this.waitForSlotAcquisition = waitForSlotAcquisition;
    this.allowedRequests = allowedRequests;
    this.isSlotBorrowingEnabled = isSlotBorrowingEnabled;
    this.definition = new RateLimiterPayload();
    definition.enabled = isEnabled;
    definition.allowedRequests = allowedRequests;
    definition.guaranteedSlots = guaranteedSlotsThreshold;
    definition.slotBorrowingEnabled = isSlotBorrowingEnabled;
    definition.slotAcquisitionTimeoutInMS = Math.toIntExact(waitForSlotAcquisition);
  }

  private static final RateLimiterPayload EMPTY = new RateLimiterPayload(); // use defaults;

  public boolean update(RateLimiterPayload definition) {
    if (definition == null) {
      definition = EMPTY; // use defaults
    }

    if (definition.equals(this.definition)) {
      return false;
    }

    this.definition = definition;

    allowedRequests = definition.allowedRequests == null ? DEFAULT_CONCURRENT_REQUESTS : definition.allowedRequests;

    isEnabled = definition.enabled == null ? false : definition.enabled; // disabled by default

    guaranteedSlotsThreshold = definition.guaranteedSlots == null ? this.allowedRequests / 2 : definition.guaranteedSlots;

    isSlotBorrowingEnabled = definition.slotBorrowingEnabled == null ? false : definition.slotBorrowingEnabled;

    waitForSlotAcquisition = definition.slotAcquisitionTimeoutInMS == null ? DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS : definition.slotAcquisitionTimeoutInMS.longValue();

    return true;
  }
}
