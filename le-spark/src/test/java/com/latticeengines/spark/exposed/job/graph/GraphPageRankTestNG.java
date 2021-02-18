package com.latticeengines.spark.exposed.job.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.graph.GraphPageRankJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GraphPageRankTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GraphPageRankTestNG.class);

    private static final List<Pair<String, Class<?>>> VERTEX_FIELDS = Arrays.asList( //
            Pair.of("id", String.class), //
            Pair.of("name", String.class), //
            Pair.of("age", Integer.class));

    private static final List<Pair<String, Class<?>>> EDGE_FIELDS = Arrays.asList( //
            Pair.of("src", String.class), //
            Pair.of("dst", String.class), //
            Pair.of("relationship", String.class));

    List<String> inputs = new ArrayList<>();

    @Test(groups = "functional")
    public void runTest() {
        prepareData();
        GraphPageRankJobConfig config = new GraphPageRankJobConfig();
        SparkJobResult result = runSparkJob(GraphPageRankJob.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
    }

    private void prepareData() {
        Object[][] Vertex = new Object[][] { //
                { "a", "Alice", 34 }, //
                { "b", "Bob", 36 }, //
                { "c", "Charlie", 30 }, //
        };
        inputs.add(uploadHdfsDataUnit(Vertex, VERTEX_FIELDS));

        Object[][] edges = new Object[][] { //
                { "a", "b", "friend" }, //
                { "b", "c", "follow" }, //
                { "c", "b", "follow" }, //
        };
        inputs.add(uploadHdfsDataUnit(edges, EDGE_FIELDS));
    }
}
