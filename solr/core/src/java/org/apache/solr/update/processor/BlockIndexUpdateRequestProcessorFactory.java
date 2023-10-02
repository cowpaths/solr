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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;

public class BlockIndexUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

 private boolean dedupe = false;
  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    dedupe = "true".equals(args._get("dedupe", "false"));
  }

  public static final Set<String> DEDUPED_FLDS = Set.of(
          "PageCountry",
          "UserEmail",
          "UserAppKey",
          "PageScreenWidth",
          "UserId",
          "PageOperatingSystem",
          "PageRefererUrlHost",
          "UserCreated",
          "UserDisplayName",
          "PageAgent",
          "PageBrowser",
          "SessionStart",
          "PageUrlHost",
          "PageDevice",
          "PageLatLongQuadStr",
          "PageLatLongQuad",
          "PageRegion",
          "PagePlatform",
          "PageIp",
          "PageCity",
          "SessionTipDeleted",
          "AppDeviceModel",
          "AppOsVersion",
          "SessionTipActiveSec",
          "AppPackageName",
          "AppName",
          "AppVersion",
          "SessionTipTotalSec",
          "AppFsVersion",
          "SessionTipNumEvents",
          "AppDeviceVendor",
          "PageName",
          "SessionTipNumPages",
          "EventWebSourceFileUrlHost",
          "PageViewportWidth",
          "IndvSample",
          "PageBrowserVersion",
          "CaptureSourceIntegration"

  );
  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new URP(req, rsp, next);
  }

  private class URP extends UpdateRequestProcessor {
    final boolean isEnabled;
    final boolean dedupe ;
    private Map<String, SolrInputDocument> sessions = new LinkedHashMap<>();
    private Map<String, List<SolrInputDocument>> events = new LinkedHashMap<>();
    private final SolrQueryRequest req;

    public URP(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      super(next);
      this.req = req;
      isEnabled = true;
      if(req.getParams().get("dedupeEventFields") !=null){
        dedupe = req.getParams().getBool("dedupeEventFields", false);
      } else {
        dedupe = BlockIndexUpdateRequestProcessorFactory.this.dedupe;
      }

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
          String sessionId = String.valueOf(cmd.solrDoc.getFieldValue("SessionId"));
          sessions.put(sessionId, cmd.solrDoc);
          //maybe some events came before the session itself
          List<SolrInputDocument> ev = events.remove(sessionId);
          if(ev!= null) {
            for (SolrInputDocument e : ev) {
              dedupeFields(cmd.solrDoc, e);
            }
            cmd.solrDoc.addField("events", ev);
          }
          return;
        }
        if ("event".equals(kind)) {
          String session = String.valueOf(cmd.solrDoc.getFieldValue("SessionId"));
          if (session != null) {
            SolrInputDocument s = sessions.get(session);
            if (s != null) {
              s.addField("events", cmd.solrDoc);
              dedupeFields(s, cmd.solrDoc);
            } else {
              events.computeIfAbsent(session, s1 -> new ArrayList<>()).add(cmd.solrDoc);
            }
          } else {
            // no sessionId in the event. It shouldn't happen
            super.processAdd(cmd);
          }
        } else{
          super.processAdd(cmd);
        }
      }
    }

    private void dedupeFields(SolrInputDocument session, SolrInputDocument event) {
      if(!dedupe) return;
      for (String fld : DEDUPED_FLDS) {
        SolrInputField f = event.removeField(fld);
        if(f != null) {
          SolrInputField old = session.getField(fld);
          if(old == null) {
            session.addField(fld, f.getValue());
          }
        }
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
        Map<String, SolrInputDocument> sessionCopy = sessions;
        sessions = new HashMap<>();
        for (SolrInputDocument d : sessionCopy.values()) {
          AddUpdateCommand add = new AddUpdateCommand(req);
          add.solrDoc = d;
          next.processAdd(add);
        }
      }
      if (!events.isEmpty()) {
        Map<String, List<SolrInputDocument>> eventsCopy = events;
        events = new HashMap<>();
        for (Map.Entry<String, List<SolrInputDocument>> e : eventsCopy.entrySet()) {
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
      }
    }
  }
}
