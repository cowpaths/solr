package org.apache.solr.update.processor;

import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class DocumentNester {
    public static final Set<String> DEDUPED_FLDS_ALL = Set.of(
            "PageLatLongQuad",
            "PageCountry",
            "PageRegion",
            "PageCity",
            "PageAgent",
            "UserDisplayName",
            "UserEmail",
            "PageRefererUrl",
            "PageBrowserVersionRange",
            "PageDevice",
            "PageStart",
            "PageRefererUrlHost",
            "PageNumInfos",
            "DefinedEventId",
            "PagePlatform",
            "PageViewId",
            "UserAppKey",
            "IndvSample",
            "PageName",
            "PageUrlPathStr",
            "LoadFirstPaintTime",
            "SessionStart",
            "UserIdSessionIdPageId",
            "PageDefId",
            "PageNumWarnings",
            "PageScreenHeight",
            "PageViewportWidth",
            "LoadDomContentTime",
            "PageUrlHost",
            "PageNumErrors",
            "UserId",
            "PageOperatingSystem",
            "PageId",
            "PageViewportHeight",
            "PageRefererUrlPathStr",
            "PageIp",
            "UserIdSessionId",
            "PageIpRange",
            "PageBrowserVersion",
            "UserCreated",
            "PageUrl",
            "PageScreenWidth",
            "PageBrowser",
            "PageMaxScrollDepthPercent");

    public static final Set<String> DEDUPED_FLDS_PAGE = Set.of(
            "PageLatLongQuad",
            "PageCountry",
            "PageRegion",
            "PageCity",
            "PageAgent",
            "PageRefererUrl",
            "PageBrowserVersionRange",
            "PageDevice",
            "PageStart",
            "PageRefererUrlHost",
            "PageNumInfos",
            "DefinedEventId",
            "PagePlatform",
            "PageViewId",
            "UserAppKey",
            "PageName",
            "PageUrlPathStr",
            "LoadFirstPaintTime",
            "UserIdSessionIdPageId",
            "PageDefId",
            "PageNumWarnings",
            "PageScreenHeight",
            "PageViewportWidth",
            "LoadDomContentTime",
            "PageUrlHost",
            "PageNumErrors",
            "PageOperatingSystem",
            "PageId",
            "PageViewportHeight",
            "PageRefererUrlPathStr",
            "PageIp",
            "PageIpRange",
            "PageBrowserVersion",
            "PageUrl",
            "PageScreenWidth",
            "PageBrowser",
            "PageMaxScrollDepthPercent");

    //sessionId Vs doc
    Map<String, Session> sessions = new HashMap<>();

    static class Session {
        final String id;
        Map<String, Page> pages = new HashMap<>();
        SolrInputDocument d;

        Session(String id) {
            this.id = id;
        }

        private static class Page {
            final String pageId;
            SolrInputDocument d;
            List<SolrInputDocument> events = new ArrayList<>();

            private Page(String pageId) {
                this.pageId = pageId;
            }
        }
    }

    private final Function<String, SolrInputDocument> docFethcer;
    private final Consumer<SolrInputDocument> docSink;

    public DocumentNester(
            Function<String, SolrInputDocument> docFethcer,
            Consumer<SolrInputDocument> docSink) {
        this.docFethcer = docFethcer;
        this.docSink = docSink;
    }

    public void add(SolrInputDocument doc) {
        String kind = (String) doc.getFieldValue("Kind");
        if ("session".equals(kind)) {
            String sessionId = String.valueOf(doc.getFieldValue("SessionId"));
            Session s = new Session(sessionId);

            s.d = doc;
            sessions.put(sessionId, s);
            return;
            //maybe some pages came before the session itself

        }
        if ("page".equals(kind)) {
            String session = String.valueOf(doc.getFieldValue("SessionId"));
            String pageId = String.valueOf(doc.getFieldValue("PageId"));
            sessions.computeIfAbsent(session, Session::new)
                    .pages.computeIfAbsent(pageId, Session.Page::new).d = doc;
        }
        if ("event".equals(kind)) {
            String session = String.valueOf(doc.getFieldValue("SessionId"));
            String pageId = String.valueOf(doc.getFieldValue("PageId"));
            sessions.computeIfAbsent(session, Session::new)
                    .pages.computeIfAbsent(pageId, Session.Page::new).events.add(doc);

        }

    }

    public void close() throws IOException {

        if (!sessions.isEmpty()) {
            Map<String, Session> sessionCopy = sessions;
            sessions = new HashMap<>();
            for (Map.Entry<String, Session> s : sessionCopy.entrySet()) {
                SolrInputDocument session = s.getValue().d;
                for (Map.Entry<String, Session.Page> p : s.getValue().pages.entrySet()) {
                    SolrInputDocument pag = p.getValue().d;
                    if (session == null) {
                        docSink.accept(pag);
                    } else {
                        session.addField("pages", pag);
                    }
                    for (SolrInputDocument event : p.getValue().events) {
                        if (pag == null || session == null) {
                            docSink.accept(event);
                        } else {
                            dedupeVals(session
                                    , pag,
                                    event);
                            pag.addField("events", event);
                        }
                    }
                }
                if (session != null) {
                    docSink.accept(session);
                }
            }
        }
    }

    private void dedupeVals(SolrInputDocument s, SolrInputDocument p, SolrInputDocument e) {
        for (String fName : DEDUPED_FLDS_ALL) {
            Object f = e.getFieldValue(fName);
            if (f == null) continue;
            e.removeField(fName);
            if (DEDUPED_FLDS_PAGE.contains(fName)) {
                // this field belongs to page
                Object valInPage = p.getFieldValue(fName);
                if (valInPage == null) p.addField(fName, f);
            } else {
                // this field belongs in session
                Object valInSession = s.getFieldValue(fName);
                if (valInSession == null) {
                    s.addField(fName, f);
                }
            }
        }

        for (String fName : DEDUPED_FLDS_ALL) {
            if (DEDUPED_FLDS_PAGE.contains(fName)) {
                continue;
            }
            Object f = p.getFieldValue(fName);
            if (f == null) continue;
            p.removeField(fName);
            Object valInSession = s.getFieldValue(fName);
            if (valInSession == null) {
                s.addField(fName, f);
            }
        }
        if(e.getFieldValue("Id") == null) {
            System.out.println("NOIDEV");

        }
        if(p.getFieldValue("Id") == null) {
            System.out.println("NOIDPG");

        }
        if(s.getFieldValue("Id") == null) {
            System.out.println("NOIDS");

        }
    }


}
