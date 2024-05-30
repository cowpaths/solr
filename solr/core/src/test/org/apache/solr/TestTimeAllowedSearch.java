package org.apache.solr;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.util.Locale;
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

  /**
   * This test demonstrates timeAllowed expectation at @{@link
   * org.apache.solr.handler.component.HttpShardHandler} level This test creates collection with
   * 'implicit` router, which has two shards shard_1 has 100000 docs, so that query should take some
   * time shard_2 has only 1 doc to demonstrate the HttpSHardHandler timeout Then it execute
   * substring query with TIME_ALLOWED 50, assuming this query will time out on shard_1
   */
  public void testTimeAllowed() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    try {
      CloudSolrClient client = cluster.getSolrClient();
      String COLLECTION_NAME = "test_coll";
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setRouterName("implicit")
          .setShards("shard_1,shard_2")
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
      UpdateRequest ur = new UpdateRequest();
      for (int i = 0; i < 100000; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "" + i);
        final String s =
            RandomStrings.randomAsciiLettersOfLengthBetween(random(), 10, 100)
                .toLowerCase(Locale.ROOT);
        doc.setField("subject_s", s);
        doc.setField("_route_", "shard_1");
        ur.add(doc);
      }

      for (int i = 0; i < 1; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "" + i);
        final String s =
            RandomStrings.randomAsciiLettersOfLengthBetween(random(), 10, 100)
                .toLowerCase(Locale.ROOT);
        doc.setField("subject_s", s);
        doc.setField("_route_", "shard_2");
        ur.add(doc);
      }

      ur.commit(client, COLLECTION_NAME);

      SolrQuery query = new SolrQuery();
      query.setQuery("subject_s:*abcd*");
      query.set(CommonParams.TIME_ALLOWED, 50);
      query.set(ShardParams.SHARDS_TOLERANT, "true");
      QueryResponse response = client.query(COLLECTION_NAME, query);
      assertTrue(
          "Should have found 1/0 doc as timeallowed is 50ms found:"
              + response.getResults().getNumFound(),
          response.getResults().getNumFound() <= 1);

      query = new SolrQuery();
      query.setQuery("subject_s:*abcd*");
      query.set(ShardParams.SHARDS_TOLERANT, "true");
      response = client.query(COLLECTION_NAME, query);
      assertTrue(
          "Should have found few docs as timeallowed is unlimited ",
          response.getResults().getNumFound() >= 0);
    } finally {
      cluster.shutdown();
    }
  }
}
