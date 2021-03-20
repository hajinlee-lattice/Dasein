package com.latticeengines.spark.exposed.job.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
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
    
    private HdfsFileFilter avroFileFilter = new HdfsFileFilter() {
        @Override
        public boolean accept(FileStatus file) {
            return file.getPath().getName().endsWith("avro");
        }
    };

    List<String> inputs = new ArrayList<>();

    @Test(groups = "functional")
    public void runTest() throws Exception {
        prepareData();
        AssignEntityIdsJobConfig config = new AssignEntityIdsJobConfig();
        SparkJobResult result = runSparkJob(AssignEntityIdsJob.class, config, inputs, getWorkspace());
        log.info("Result = {}", JsonUtils.serialize(result));
        log.info("Output: " + result.getOutput());
        //printIds(result.getTargets().get(1).getPath());
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
                { 8L, docV, "DW1", "SF000045", "component03" }, //
                { 11L, idV, "DW2AccountID", "DW2000778", "component01" }, //
                { 12L, idV, "DW2AccountID", "DW2000790", "component01" }, //
                { 13L, idV, "SalesforceAccountID", "SF000350", "component01" }, //
                { 14L, idV, "SalesforceAccountID", "SF000165", "component02" }, //
                { 15L, idV, "DW1AccountID", "DW1000118", "component02" }, //
                { 16L, idV, "DW2AccountID", "DW2001290", "component03" }, //
                { 17L, idV, "SalesforceAccountID", "SF004455", "component03" }, //
                { 18L, idV, "DW1AccountID", "DW1000918", "component03" }, //
                { 19L, idV, "DW1AccountID", "DW1004358", "component03" }, //
                { 20L, idV, "DW2AccountID", "DW2000338", "component03" }
        };
        inputs.add(uploadHdfsDataUnit(vertices, VERTEX_FIELDS));

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
                { 8L, 20L, "{}" }
        };
        inputs.add(uploadHdfsDataUnit(edges, EDGE_FIELDS));
    }

    private void printIds(String hdfsDir) throws Exception {
        List avroFilePaths = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);
        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();

            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String src = getString(record, "src");
                    String dst = getString(record, "dst");
                    log.info(String.format("src=%s, dst=%s", src, dst));
                }
            }
        }
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
