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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.BeforeClass;

public class BlockUpdateTest extends UpdateProcessorTestBase {
  public static final String CONFIG_XML = "solrconfig-block-update-processor.xml";
  public static final String SCHEMA_XML = "schema-block-update.xml";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(CONFIG_XML, SCHEMA_XML);
  }

  public void testBulkUpdate() throws Exception {
    processAdds(
        "block-update",
        params("sessionBlock", "true", "commit", "true"),
        doc(f("Id", "11"), f("desc", "Session 11"), f("Kind", "session")),
        doc(f("Id", "11!1"), f("desc", "Event 11!1"), f("Kind", "event"), f("SessionId", "11")));
    assertJQ(req("Id:11!1"), "response/docs/[0]/_nest_parent_==\"11\"");
    processAdds(
        "block-update",
        params("sessionBlock", "true", "commit", "true"),
        doc(f("Id", "11!2"), f("desc", "Event 11!2"), f("Kind", "event"), f("SessionId", "11")),
        doc(f("Id", "11!3"), f("desc", "Event 11!3"), f("Kind", "event"), f("SessionId", "11")));
    assertJQ(req("Id:11!1"), "response/docs/[0]/_nest_parent_==\"11\"");
    assertJQ(req("Id:11!2"), "response/docs/[0]/_nest_parent_==\"11\"");
    assertJQ(req("Id:11!3"), "response/docs/[0]/_nest_parent_==\"11\"");
  }

  /**
   * Runs a document through the specified chain, and returns the final document used when the chain
   * is completed (NOTE: some chains may modify the document in place
   */
  protected void processAdds(
      final String chain, final SolrParams requestParams, final SolrInputDocument... docs)
      throws IOException {

    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);

    SolrQueryResponse rsp = new SolrQueryResponse();

    SolrQueryRequest req = new LocalSolrQueryRequest(core, requestParams);
    UpdateRequestProcessor processor = pc.createProcessor(req, rsp);

    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      for (SolrInputDocument doc : docs) {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        processor.processAdd(cmd);
      }
      if (requestParams.getBool("commit", false)) {
        processor.processCommit(new CommitUpdateCommand(req, false));
      }
    } finally {
      processor.finish();
      processor.close();
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
  }
}
