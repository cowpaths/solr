package org.apache.solr.search.facet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;

public class TestMultiFieldFacets extends SolrTestCaseHS {
    private static SolrInstances servers; // for distributed testing
    private static int origTableSize;
    private static FacetField.FacetMethod origDefaultFacetMethod;

    @SuppressWarnings("deprecation")
    @BeforeClass
    public static void beforeTests() throws Exception {
        systemSetPropertySolrDisableUrlAllowList("true");
        JSONTestUtil.failRepeatedKeys = true;

        origTableSize = FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE;
        FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE = 2; // stress test resizing

        origDefaultFacetMethod = FacetField.FacetMethod.DEFAULT_METHOD;
        // instead of the following, see the constructor
        // FacetField.FacetMethod.DEFAULT_METHOD = rand(FacetField.FacetMethod.values());

        // we need DVs on point fields to compute stats & facets
        if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP))
            System.setProperty(NUMERIC_DOCVALUES_SYSPROP, "true");

        initCore("solrconfig-tlog.xml", "schema_latest.xml");
    }

    /** Start all servers for cluster if they don't already exist */
    public static void initServers() throws Exception {
        if (servers == null) {
            servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
        }
    }

    @SuppressWarnings("deprecation")
    @AfterClass
    public static void afterTests() throws Exception {
        systemClearPropertySolrDisableUrlAllowList();
        JSONTestUtil.failRepeatedKeys = false;
        FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE = origTableSize;
        FacetField.FacetMethod.DEFAULT_METHOD = origDefaultFacetMethod;
        if (servers != null) {
            servers.stop();
            servers = null;
        }
    }

    // tip: when debugging failures, change this variable to DEFAULT_METHOD
    // (or if only one method is problematic, set to that explicitly)
    private static final FacetField.FacetMethod TEST_ONLY_ONE_FACET_METHOD =
            null; // FacetField.FacetMethod.DEFAULT_METHOD;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        if (null != TEST_ONLY_ONE_FACET_METHOD) {
            return Collections.singleton(new Object[] {TEST_ONLY_ONE_FACET_METHOD});
        } else if (TEST_NIGHTLY) {
            // wrap each enum val in an Object[] and return as Iterable
            return () ->
                    Arrays.stream(FacetField.FacetMethod.values()).map(it -> new Object[] {it}).iterator();
        } else {
            // pick a single random method and test it
            FacetField.FacetMethod[] methods = FacetField.FacetMethod.values();

            // can't use LuceneTestCase.random() because we're not in the runner context yet
            String seed = System.getProperty("tests.seed", "");
            return Collections.singleton(
                    new Object[] {methods[Math.abs(seed.hashCode()) % methods.length]});
        }
    }

    public TestMultiFieldFacets(FacetField.FacetMethod defMethod) {
        FacetField.FacetMethod.DEFAULT_METHOD =
                defMethod; // note: the real default is restored in afterTests
    }
    public void testMultiFieldFacet() throws Exception {
        Client client = Client.localClient();

        client.deleteByQuery("*:*", null);
        client.add(
            sdoc(
            "id", "1", "1_s", "A", "2_s", "A", "3_s", "C", "y_s", "B", "x_t", "x   z", "z_t",
                "  2 3"),
            null);
        client.add(
            sdoc(
            "id", "2", "1_s", "B", "2_s", "A", "3_s", "B", "y_s", "B", "x_t", "x y  ", "z_t",
                "1   3"),
            null);
        client.add(
            sdoc(
            "id", "3", "1_s", "C", "2_s", "A", "3_s", "#", "y_s", "A", "x_t", "  y z", "z_t",
                "1 2  "),
            null);
        client.add(
            sdoc(
            "id", "4", "1_s", "A", "2_s", "B", "3_s", "C", "y_s", "A", "x_t", "    z", "z_t",
                "    3"),
            null);
        client.add(
            sdoc(
            "id", "5", "1_s", "B", "2_s", "_", "3_s", "B", "y_s", "C", "x_t", "x    ", "z_t",
                "1   3"),
            null);
        client.add(
            sdoc(
            "id", "6", "1_s", "C", "2_s", "B", "3_s", "A", "y_s", "C", "x_t", "x y z", "z_t",
                "1    "),
        null);
        client.commit();

        assertJQ(
            req(
                "q",
                "x_t:x",
                "rows",
                "0", //
                "json.facet",
                ""
                    + "{x: { type: terms, field: 'x_t' } }"),
    "facets=={count:4, "
            + "x:{ buckets:["
            + "  { val: x, count: 4 },"
            + "  { val: y, count: 2 },"
            + "  { val: z, count: 2 },"
            + "   ]}}");
    }
}
