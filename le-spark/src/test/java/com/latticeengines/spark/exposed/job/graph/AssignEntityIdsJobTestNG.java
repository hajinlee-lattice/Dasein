package com.latticeengines.spark.exposed.job.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class AssignEntityIdsJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AssignEntityIdsJobTestNG.class);

    private static final String docV = "docV";
    private static final String idV = "IdV";

    private static final List<Pair<String, Class<?>>> VERTEX_FIELDS = Arrays.asList( //
            Pair.of("id", Long.class), //
            Pair.of("type", String.class), //
            Pair.of("systemID", String.class), //
            Pair.of("vertexValue", String.class), //
            Pair.of("ConnectedComponentID", String.class));

    private static final List<Pair<String, Class<?>>> EDGE_FIELDS = Arrays.asList( //
            Pair.of("src", Long.class), //
            Pair.of("dst", Long.class), //
            Pair.of("property", String.class));

    private final List<String> edgeRank = new ArrayList<>();

    @Test(groups = "functional")
    public void runTest() throws Exception {
        prepareData();
        AssignEntityIdsJobConfig config = new AssignEntityIdsJobConfig();
        config.setEdgeRank(edgeRank);

        SparkJobResult result = runSparkJob(AssignEntityIdsJob.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));

        verify(result, Arrays.asList(this::verifyVertices, this::verifyEdges, this::verifyReport));
    }

    private void prepareData() {
        Object[][] vertices = new Object[][] { //
                { 1L, docV, "Salesforce", "SF000022", "component01" }, //
                { 2L, docV, "DW1", "DW1000043", "component01" }, //
                { 3L, docV, "Salesforce", "SF000045", "component02" }, //
                { 4L, docV, "DW1", "DW1000098", "component02" }, //
                { 5L, docV, "DW1", "SF000045", "component03" }, //
                { 6L, docV, "Salesforce", "SF000045", "component03" }, //
                { 7L, docV, "DW2", "SF000045", "component03" }, //
                { 8L, docV, "Salesforce", "SF000015", "component03" }, //
                { 11L, idV, "DW2AccountID", "DW2000778", "component01" }, //
                { 12L, idV, "DW2AccountID", "DW2000790", "component01" }, //
                { 13L, idV, "SalesforceAccountID", "SF000350", "component01" }, //
                { 14L, idV, "SalesforceAccountID", "SF000165", "component02" }, //
                { 15L, idV, "DW1AccountID", "DW1000118", "component02" }, //
                { 16L, idV, "SalesforceAccountID", "SF2001290", "component03" }, //
                { 17L, idV, "DW2AccountID", "DW2004455", "component03" }, //
                { 18L, idV, "DW1AccountID", "DW1000918", "component03" }, //
                { 19L, idV, "DW1AccountID", "DW1004358", "component03" }, //
                { 20L, idV, "SalesforceAccountID", "SF2000338", "component03" }, //
                { 30L, docV, "Salesforce", "SF2000341", "component04" }, //
                { 31L, docV, "Salesforce", "SF2001435", "component04" }, //
                { 32L, docV, "Salesforce", "SF2003423", "component04" }, //
                { 33L, docV, "Salesforce", "SF2000742", "component04" }, //
                { 40L, idV, "DW2AccountID", "DW2000587", "component04" }, //
                { 41L, idV, "DW2AccountID", "DW2000674", "component04" }, //
                { 42L, idV, "DW2AccountID", "DW2000367", "component04" }, //
                { 43L, idV, "DW2AccountID", "DW2000457", "component04" }, //
                { 44L, idV, "DW2AccountID", "DW2000874", "component04" }, //
        };
        uploadHdfsDataUnit(vertices, VERTEX_FIELDS);

        Object[][] edges = new Object[][] { //
                { 1L, 11L, "{}" }, //
                { 1L, 13L, "{}" }, //
                { 2L, 12L, "{}" }, //
                { 2L, 13L, "{}" }, //
                { 3L, 14L, "{}" }, //
                { 3L, 15L, "{}" }, //
                { 4L, 14L, "{}" }, //
                { 4L, 15L, "{}" }, //
                { 5L, 16L, "{}" }, //
                { 5L, 17L, "{}" }, //
                { 6L, 16L, "{}" }, //
                { 6L, 19L, "{}" }, //
                { 7L, 16L, "{}" }, //
                { 7L, 17L, "{}" }, //
                { 7L, 18L, "{}" }, //
                { 8L, 18L, "{}" }, //
                { 8L, 20L, "{}" }, //
                { 30L, 40L, "{}" }, //
                { 30L, 41L, "{}" }, //
                { 31L, 41L, "{}" }, //
                { 31L, 42L, "{}" }, //
                { 32L, 40L, "{}" }, //
                { 32L, 43L, "{}" }, //
                { 33L, 43L, "{}" }, //
                { 33L, 44L, "{}" }
        };
        uploadHdfsDataUnit(edges, EDGE_FIELDS);

        // docV-idV ranking, in descending confidence
        edgeRank.add("Salesforce-SalesforceAccountID");
        edgeRank.add("DW1-DW1AccountID");
        edgeRank.add("DW2-DW2AccountID");
        edgeRank.add("Salesforce-DW1AccountID");
        edgeRank.add("Salesforce-DW2AccountID");
        edgeRank.add("DW1-SalesforceAccountID");
        edgeRank.add("DW1-DW2AccountID");
        edgeRank.add("DW2-SalesforceAccountID");
        edgeRank.add("DW2-DW1AccountID");
    }

    private Boolean verifyVertices(HdfsDataUnit df) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        AtomicInteger count = new AtomicInteger(0);
        Set<String> entityIds = new HashSet<>();
        iterator.forEachRemaining(record -> {
            // System.out.println(record);

            count.incrementAndGet();
            String entityId = getString(record, "entityID");
            if (entityId != null) {
                entityIds.add(entityId);
            }
        });

        // Should have 27 vertices because we should remove none
        Assert.assertEquals(count.get(), 27);

        // Should have 5 unique entity IDs
        // From 4 connected components, we split two of them into two:
        // cut (7 -> 18) to split component03 into two entities
        // cut (2 -> 12) to split component01 into two entities (one of them only have the IdV-DW2AccountID-DW2000790)
        // Also, we don't assign entity ID to garbage collector components
        Assert.assertEquals(entityIds.size(), 5);

        return true;
    }

    private Boolean verifyEdges(HdfsDataUnit df) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        AtomicInteger count = new AtomicInteger(0);
        iterator.forEachRemaining(record -> {
            // System.out.println(record);
            count.incrementAndGet();
        });
        // Should have 15 edges because we start with 25,
        // remove 2: (7 -> 18) and (2 -> 12) in tie-breaking, and
        // remove 8: all edges in component04 as garbage collector
        Assert.assertEquals(count.get(), 15);
        return true;
    }

    private Boolean verifyReport(HdfsDataUnit df) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        AtomicInteger count = new AtomicInteger(0);
        iterator.forEachRemaining(record -> {
            // System.out.println(record);
            count.incrementAndGet();
        });
        // 6 rows with TieBreakingProcess +
        // 9 rows with GarbageCollector
        Assert.assertEquals(count.get(), 15);
        return true;
    }

    private static String getString(GenericRecord record, String field) {
        Object obj = record.get(field);
        if (obj == null) {
            return null;
        } else {
            return obj.toString();
        }
    }
}
