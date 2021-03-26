package com.latticeengines.spark.exposed.job.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.graph.MergeGraphsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeGraphsJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(MergeGraphsJobTestNG.class);

    private static final String VERTEX_ID = "id";
    private static final String VERTEX_TYPE = "type";
    private static final String VERTEX_SYSTEM_ID = "systemID";
    private static final String VERTEX_VALUE = "vertexValue";

    private static final String EDGE_SRC = "src";
    private static final String EDGE_DEST = "dst";
    private static final String EDGE_PROPERTY = "property";

    private static final String DocV = "docV";
    private static final String IdV = "IdV";
    private static final String CONNECTED_COMPONENT_ID = "ConnectedComponentID";

    private static final List<Pair<String, Class<?>>> VERTEX_FIELDS = Arrays.asList( //
            Pair.of(VERTEX_ID, Long.class), //
            Pair.of(VERTEX_TYPE, String.class), //
            Pair.of(VERTEX_SYSTEM_ID, String.class), //
            Pair.of(VERTEX_VALUE, String.class));

    private static final List<Pair<String, Class<?>>> EDGE_FIELDS = Arrays.asList( //
            Pair.of(EDGE_SRC, Long.class), //
            Pair.of(EDGE_DEST, Long.class), //
            Pair.of(EDGE_PROPERTY, String.class));

    @Inject
    private Configuration yarnConfiguration;

    private HdfsFileFilter avroFileFilter = new HdfsFileFilter() {
        @Override
        public boolean accept(FileStatus file) {
            return file.getPath().getName().endsWith("avro");
        }
    };

    private List<String> inputs = new ArrayList<>();
    private List<Map<String, String>> inputDescriptors = new ArrayList<>();

    @Test(groups = "functional")
    public void runTest() throws Exception {
        prepareData();
        MergeGraphsJobConfig config = new MergeGraphsJobConfig();
        SparkJobResult result = runSparkJob(MergeGraphsJob.class, config, inputs, getWorkspace());
        log.info("Result = {}", JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);

        // From 15 vertices, 3 should be removed as duplicates
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 12);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 9);

        List<String> componentsIds = getComponentsIds(result.getTargets().get(0).getPath());
        Assert.assertEquals(componentsIds.size(), 12);

        // 3 Connected Components are expected from the test case
        Set<String> uniqueComponentsIds = new HashSet<>(componentsIds);
        Assert.assertEquals(uniqueComponentsIds.size(), 3);
    }

    private void prepareData() {
        log.info("Preparing inputs...");

        Object[][] vertexDf1 = new Object[][] { //
            {1L, DocV, "Salesforce", "SF000022"}, //
            {2L, DocV, "Salesforce", "SF000045"}, //
            {3L, DocV, "Salesforce", "SF000075"}, //
            {11L, IdV, "SalesforceAccountID", "SF000022"}, //
            {12L, IdV, "SalesforceAccountID", "SF000045"}, //
            {13L, IdV, "SalesforceAccountID", "SF000075"}
        };
        inputs.add(uploadHdfsDataUnit(vertexDf1, VERTEX_FIELDS));

        Object[][] edgeDf1 = new Object[][] { //
            {1L, 11L, "{}"}, //
            {2L, 12L, "{}"}, //
            {3L, 13L, "{}"}
        };
        inputs.add(uploadHdfsDataUnit(edgeDf1, EDGE_FIELDS));

        Object[][] vertexDf2 = new Object[][] { //
            {21L, DocV, "DW1", "DW1000043"}, //
            {22L, DocV, "DW1", "DW1000098"}, //
            {31L, IdV, "SalesforceAccountID", "SF000022"}, //
            {32L, IdV, "SalesforceAccountID", "SF000075"}, //
            {33L, IdV, "DW1AccountID", "DW1000043"}, //
            {34L, IdV, "DW1AccountID", "DW1000098"}
        };
        inputs.add(uploadHdfsDataUnit(vertexDf2, VERTEX_FIELDS));

        Object[][] edgeDf2 = new Object[][] { //
            {21L, 31L, "{}"}, //
            {21L, 33L, "{}"}, //
            {22L, 32L, "{}"}, //
            {22L, 34L, "{}"}
        };
        inputs.add(uploadHdfsDataUnit(edgeDf2, EDGE_FIELDS));

        Object[][] vertexDf3 = new Object[][] { //
            {41L, DocV, "DW2", "SF000022"}, //
            {51L, IdV, "DW1AccountID", "DW1000043"}, //
            {52L, IdV, "DW2AccountID", "DW2000058"}
        };
        inputs.add(uploadHdfsDataUnit(vertexDf3, VERTEX_FIELDS));

        Object[][] edgeDf3 = new Object[][] { //
            {41L, 51L, "{}"}, //
            {41L, 52L, "{}"}
        };
        inputs.add(uploadHdfsDataUnit(edgeDf3, EDGE_FIELDS));
    }

    private List<String> getComponentsIds(String hdfsDir) throws Exception {
        List<String> componentIds = new ArrayList<>();
        List avroFilePaths = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);
        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();

            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String componentId = getString(record, CONNECTED_COMPONENT_ID);
                    String vertexId = getString(record, VERTEX_ID);
                    if (!componentId.isEmpty()) {
                        log.info(String.format("vertexId=%s, componentId=%s", vertexId, componentId));
                        componentIds.add(componentId);
                    }
                }
            }
        }

        return componentIds;
    }

    private static String getString(GenericRecord record, String field) throws Exception {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = "";
        }
        return value;
    }
}
