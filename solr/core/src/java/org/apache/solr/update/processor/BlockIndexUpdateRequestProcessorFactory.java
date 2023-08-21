package org.apache.solr.update.processor;

import java.io.IOException;
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

public class BlockIndexUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

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
      isEnabled = req.getParams().getBool("sessionBlock", false);
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
          sessions.put((String) cmd.solrDoc.getFieldValue("Id"), cmd.solrDoc);
          return;
        }
        if ("event".equals(kind)) {
          String session = (String) cmd.solrDoc.getFieldValue("SessionId");
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
