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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockIndexUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new URP(req, rsp, next);
  }

  private class URP extends UpdateRequestProcessor {
    final boolean isEnabled;
    private Map<String, SolrInputDocument> sessions = new LinkedHashMap<>();
    private Map<String, List<SolrInputDocument>> events = new LinkedHashMap<>();
    private final SolrQueryRequest req;

    public URP(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      super(next);
      this.req = req;
      isEnabled = true;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (!isEnabled) {
        super.processAdd(cmd);
        return;
      }
      if (cmd.solrDoc != null) {
        String kind = (String) cmd.solrDoc.getFieldValue("Kind");
        if ("session".equals(kind)) {
          sessions.put(String.valueOf(cmd.solrDoc.getFieldValue("SessionId")), cmd.solrDoc);
          return;
        }
        if ("event".equals(kind)) {
          String session = String.valueOf(cmd.solrDoc.getFieldValue("SessionId"));
          if (session != null) {
            SolrInputDocument s = sessions.get(session);
            if (s != null) {
              s.addField("events", cmd.solrDoc);
            } else {
              events.computeIfAbsent(session, s1 -> new ArrayList<>()).add(cmd.solrDoc);
            }
          } else {
            // no sessionId in the event. It shouldn't happen
            super.processAdd(cmd);
          }
        } else super.processAdd(cmd);
      }
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      processNested();
      super.processCommit(cmd);
    }

    @Override
    public void finish() throws IOException {
      if (!isEnabled) {
        super.finish();
        return;
      }
      processNested();
      next.finish();
    }

    private void processNested() throws IOException {
      if (!sessions.isEmpty()) {
        for (SolrInputDocument d : sessions.values()) {
          AddUpdateCommand add = new AddUpdateCommand(req);
          add.solrDoc = d;
          next.processAdd(add);
        }
        sessions.clear();
      }
      if (!events.isEmpty()) {
        for (Map.Entry<String, List<SolrInputDocument>> e : events.entrySet()) {
          BytesRef session = new BytesRef(e.getKey());
          final SolrInputDocument oldSessionWithoutChildren =
              RealTimeGetComponent.getInputDocument(
                  req.getCore(),
                  session,
                  session,
                  null,
                  null,
                  RealTimeGetComponent.Resolution.DOC); // when no children, just fetches the doc
          if (oldSessionWithoutChildren == null) {
            // strange. The session does not exist. just index as is
            for (SolrInputDocument d : e.getValue()) {
              AddUpdateCommand add = new AddUpdateCommand(req);
              add.solrDoc = d;
              next.processAdd(add);
            }

          } else {
            oldSessionWithoutChildren.addField("events", Map.of("add", e.getValue()));
            AddUpdateCommand add = new AddUpdateCommand(req);
            add.solrDoc = oldSessionWithoutChildren;
            next.processAdd(add);
          }
        }
        events.clear();
      }
    }
  }
}
