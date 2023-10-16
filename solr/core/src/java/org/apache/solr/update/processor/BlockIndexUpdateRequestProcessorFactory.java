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

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockIndexUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private boolean dedupe = false;

    @Override
    public void init(NamedList<?> args) {
        super.init(args);
        dedupe = "true".equals(args._get("dedupe", "false"));
    }


    @Override
    public UpdateRequestProcessor getInstance(
            SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new URP(req, rsp, next);
    }

    private class URP extends UpdateRequestProcessor {
        final boolean isEnabled;
        final boolean dedupe;
        private final DocumentNester documentNester;
        private final SolrQueryRequest req;

        public URP(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
            super(next);
            this.req = req;
            isEnabled = true;
            if (req.getParams().get("dedupeEventFields") != null) {
                dedupe = req.getParams().getBool("dedupeEventFields", false);
            } else {
                dedupe = BlockIndexUpdateRequestProcessorFactory.this.dedupe;
            }
            documentNester = new DocumentNester(
                    k -> fetchDoc(k),
                    d -> index(req, next, d)
            );

        }

        private SolrInputDocument fetchDoc(String k) {
            try {
                BytesRef session = new BytesRef(k);
                return RealTimeGetComponent.getInputDocument(
                        req.getCore(),
                        session,
                        session,
                        null,
                        null,
                        RealTimeGetComponent.Resolution.DOC);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void index(SolrQueryRequest req, UpdateRequestProcessor next, SolrInputDocument d) {
            if(d == null) {
                log.warn("NULLDOC");
                return;
            }
            AddUpdateCommand add = new AddUpdateCommand(req);
            add.solrDoc = d;
            try {
                next.processAdd(add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void processAdd(AddUpdateCommand cmd) throws IOException {
            if (!isEnabled) {
                super.processAdd(cmd);
                return;
            }
            if (cmd.solrDoc != null) {
                documentNester.add(cmd.solrDoc);
            }
        }
        @Override
        public void processCommit(CommitUpdateCommand cmd) throws IOException {
            documentNester.close();
            super.processCommit(cmd);
        }

        @Override
        public void finish() throws IOException {
            if (!isEnabled) {
                super.finish();
                return;
            }
            documentNester.close();
            next.finish();
        }
    }
}
