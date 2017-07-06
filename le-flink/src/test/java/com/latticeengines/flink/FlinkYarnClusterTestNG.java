package com.latticeengines.flink;

import static com.latticeengines.flink.FlinkConstants.AM_JAR;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.flink.framework.FlinkFunctionalTestNGBase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.flink.runtime.Tokenizer;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class FlinkYarnClusterTestNG extends FlinkFunctionalTestNGBase {

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

    @Value("${dataplatform.queue.scheme:legacy}")
    private String queueScheme;

    private ContextEnvironment env;
    private String inputFile = "/tmp/flink/wiki.txt";;
    private String outputFile = "/tmp/flink/wiki_count.csv";

    @Test(groups = "functional")
    public void testDeployCluster() throws Exception {
        try {
            String queue = LedpQueueAssigner.getModelingQueueNameForSubmission();
            queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
            AbstractYarnClusterDescriptor yarnDescriptor = FlinkYarnCluster.createDescriptor(yarnConfiguration,
                    new Configuration(), "", queue);
            Map<String, String> flinkDistJar = new HashMap<>();
            flinkDistJar.put(AM_JAR, getDataflowJarPath());
            yarnDescriptor.setExtraJars(flinkDistJar);
            FlinkYarnCluster.launch(yarnDescriptor);
            YarnClusterClient clusterClient = FlinkYarnCluster.getClient();
            Assert.assertNotNull(clusterClient);
            env = FlinkYarnCluster.getExecutionEnvironment();

            prepareHdfsFiles();
            runWordCount();
            verifyOutput();
        } finally {
            FlinkYarnCluster.shutdown();
        }
    }

    private void prepareHdfsFiles() throws Exception {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("wiki.txt");
        if (HdfsUtils.fileExists(yarnConfiguration, inputFile)) {
            HdfsUtils.rmdir(yarnConfiguration, inputFile);
        }
        if (HdfsUtils.fileExists(yarnConfiguration, outputFile)) {
            HdfsUtils.rmdir(yarnConfiguration, outputFile);
        }
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, inputFile);
    }

    private void runWordCount() throws Exception {
        String fs = yarnConfiguration.get("fs.defaultFS");
        // get input data
        DataSet<String> text = env.readTextFile(fs + inputFile);

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1);
        counts.writeAsCsv(fs + "/" + outputFile, "\n", ",");
        env.execute("word count");
    }

    private void verifyOutput() throws Exception {
        Map<String, Integer> expectedCounts = new HashMap<>();
        expectedCounts.put("and", 22);
        expectedCounts.put("apache", 51);
        expectedCounts.put("data", 14);
        expectedCounts.put("flink", 49);

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, outputFile), "Output file " + outputFile + " does not exist.");
        String content = IOUtils.toString(HdfsUtils.getInputStream(yarnConfiguration, outputFile));
        CSVParser parser = CSVParser.parse(content, CSVFormat.RFC4180);
        for (CSVRecord csvRecord : parser) {
            Assert.assertEquals(csvRecord.size(), 2);
            String word = csvRecord.get(0);
            Integer count = Integer.valueOf(csvRecord.get(1));
            Assert.assertTrue(count >= 0);
            if (expectedCounts.containsKey(word)) {
                Assert.assertEquals(count, expectedCounts.get(word), "There should be " + expectedCounts.get(word) + " \"" + word + "\".");
            }
        }
    }

    private String getDataflowJarPath() throws Exception {
        String artifactVersion = versionManager.getCurrentVersion();
        String dataFlowLibDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataflow/lib/"
                : "/app/" + artifactVersion + "/dataflow/lib/";
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataFlowLibDir);
        for (String file : files) {
            if (file.contains("le-dataflow-") && file.endsWith(".jar")) {
                return file;
            }
        }
        return "/app/flink/flink.jar";
    }

}
