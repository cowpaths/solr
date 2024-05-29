package org.apache.solr;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.util.Random;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;

public class TestTimeAllowedSearch extends SolrCloudTestCase {

  public void testTimeAllowed() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    try {
      CloudSolrClient client = cluster.getSolrClient();
      String COLLECTION_NAME = "test_coll";
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 4, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 4, 4);
      UpdateRequest ur = new UpdateRequest();
      Random rd = new Random();
      for (int i = 0; i < 100; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "" + i);
        int min = rd.nextInt(100);
        final String s = RandomStrings.randomAsciiLettersOfLengthBetween(random(), min, min + 10);
        doc.setField("subject_s", s);
        ur.add(doc);
      }

      ur.commit(client, COLLECTION_NAME);

      SolrQuery query = new SolrQuery();
      query.setQuery("subject_s:*a*");
      query.set(CommonParams.TIME_ALLOWED, 1);
      QueryResponse response = client.query(COLLECTION_NAME, query);
      assertTrue(
          "Should not have found any doc as timeallowed is 1ms ",
          response.getResults().getNumFound() == 0);

      query = new SolrQuery();
      query.setQuery("subject_s:*b*");
      response = client.query(COLLECTION_NAME, query);
      System.out.println("response " + response);
      assertTrue(
          "Should have found few docs as timeallowed is unlimited ",
          response.getResults().getNumFound() > 0);

      long totalResults = response.getResults().getNumFound();

      cluster.getJettySolrRunner(1).stop();

      query = new SolrQuery();
      // executing same query but one node is down
      query.setQuery("subject_s:*b*");
      query.set(ShardParams.SHARDS_TOLERANT, "true");
      query.set(CommonParams.TIME_ALLOWED, 100);
      response = client.query(COLLECTION_NAME, query);
      System.out.println("response " + response);
      assertTrue(
          "Should have found less docs as one node is stopped ",
          response.getResults().getNumFound() < totalResults);

    } finally {
      cluster.shutdown();
    }
  }
}
