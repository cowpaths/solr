package org.apache.solr.servlet;

import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;

public class TestBucketedRateLimit extends SolrCloudTestCase {
  private static final String FIRST_COLLECTION = "c1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(FIRST_COLLECTION, configset("cloud-minimal")).configure();
  }

  public void testConfig() throws Exception {
    String config =
        "{\n"
            + "  \"rate-limiters\": {\n"
            + "    \"readBuckets\": [\n"
            + "      {\n"
            + "        \"name\": \"expensive\",\n"
            + "        \"conditions\": [{\n"
            + "          \"queryParamPattern\": {\n"
            + "            \"q\": \".*multijoin.*\"\n"
            + "          }\n"
            + "        }],\n"
            + "        \"allowedRequests\": 5,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"low\",\n"
            + "        \"conditions\": [{\n"
            + "          \"headerPattern\": {\n"
            + "            \"solr_req_priority\": \"20\"\n"
            + "          }\n"
            + "        }],\n"
            + "        \"allowedRequests\": 20,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"global\",\n"
            + "        \"conditions\": [],\n"
            + "        \"allowedRequests\": 50,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}\n"
            + "\n";
    RateLimitManager mgr =
        new RateLimitManager.Builder(
                () ->
                    new SolrZkClient.NodeData(new Stat(), config.getBytes(StandardCharsets.UTF_8)))
            .build();
    RequestRateLimiter rl = mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);
    assertTrue(rl instanceof BucketedQueryRateLimiter);
    BucketedQueryRateLimiter brl = (BucketedQueryRateLimiter) rl;
    assertEquals(3, brl.buckets.size());

    RequestRateLimiter.SlotMetadata smd =
        rl.handleRequest(
            new RequestRateLimiter.RequestWrapper() {
              @Override
              public String getParameter(String name) {
                return null;
              }

              @Override
              public String getHeader(String name) {
                if (name.equals("solr_req_priority")) return "20";
                else return null;
              }
            });

    // star
    assertEquals(19, smd.usedPool.availablePermits());
    smd.decrementRequest();
    assertEquals(20, smd.usedPool.availablePermits());
  }
}
