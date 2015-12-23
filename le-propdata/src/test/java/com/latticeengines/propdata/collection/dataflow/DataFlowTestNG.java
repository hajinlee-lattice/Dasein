package com.latticeengines.propdata.collection.dataflow;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;



@Component
public class DataFlowTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private TestDataFlowService testDataFlowService;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private String domainAvroDir = "/user/build/Domain";

    @Test(groups = "functional", enabled = false)
    public void testAFunction() throws Exception {
        prepareDomainTestData();

        String sourcePath = domainAvroDir + "/*.avro";
        String outputPath = "/user/build/output";
        String dataFlowBean = "functionDataFlowBuilder";

        HdfsUtils.rmdir(yarnConfiguration, outputPath);

        testDataFlowService.executeFunctionalDataflow(sourcePath, outputPath, dataFlowBean);

        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, outputPath + "/*.avro");
        String avroPath = files.get(0);
        if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
            System.out.println(schema);
        }
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroPath);
        for (GenericRecord record: records) {
            System.out.println(record.get("Domain"));
        }

    }

    private void prepareDomainTestData() {
        String[] domains = new String[] {
                "google.com",
                "http://www.hugedomains.com/domain_profile.cfm?d=whitesidedesigns&e=com",
                "http://www.rutherfordproperty.co.nz",
                "http://trinid.com",
                "maps.google.com",
                "www.www.com"
        };

        List<GenericRecord> records =  new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," +
                "\"fields\":[{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]}]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (String domain: domains) {
            builder.set("Domain", domain);
            records.add(builder.build());
        }

        try {
            AvroUtils.writeToLocalFile(schema, records, "domain.avro");
            if (HdfsUtils.fileExists(yarnConfiguration, domainAvroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, domainAvroDir);
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, "domain.avro", domainAvroDir + "/domain.avro");
        } catch (Exception e) {
            Assert.fail("Failed to upload Domain.avro", e);
        }

        FileUtils.deleteQuietly(new File("domain.avro"));
    }

    @Test(groups = "functional", enabled = false)
    public void testMostRecentFlow() throws Exception {
        prepareDatedTestData(domainAvroDir, "domain.avro");

        String sourcePath = domainAvroDir + "/*.avro";
        String outputPath = "/user/build/output";
        String dataFlowBean = "mostRecentDataFlow";

        HdfsUtils.rmdir(yarnConfiguration, outputPath);

        testDataFlowService.executeFunctionalDataflow(sourcePath, outputPath, dataFlowBean);

        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, outputPath + "/*.avro");
        String avroPath = files.get(0);
        if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
            System.out.println(schema);
        }
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroPath);
        for (GenericRecord record: records) {
            System.out.println(String.format("%s - %s - %s",
                    record.get("Domain"), record.get("Feature"), record.get("Timestamp")));
        }

    }


    private void prepareDatedTestData(String avroDir, String fileName) {
        Object[][] data = new Object[][] {
                {"dom1.com", "feature3", 123L},
                {"dom1.com", "feature1", 125L},
                {"dom1.com", "feature2", 124L},
                {"dom2.com", "featureA", 101L},
                {"dom2.com", "featureB", 102L}
        };

        List<GenericRecord> records =  new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," +
                "\"fields\":[" +
                    "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"],\"sqlType\":\"-9\",\"columnName\":\"Domain\"}," +
                    "{\"name\":\"Feature\",\"type\":[\"string\",\"null\"],\"sqlType\":\"-9\",\"columnName\":\"Feature\"}," +
                    "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"],\"sqlType\":\"93\",\"columnName\":\"Timestamp\"}" +
                "]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] pair: data) {
            builder.set("Domain", pair[0]);
            builder.set("Feature", pair[1]);
            builder.set("Timestamp", pair[2]);
            records.add(builder.build());
        }

        try {
            AvroUtils.writeToLocalFile(schema, records, fileName);
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir);
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, fileName, avroDir + "/" + fileName);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }

        FileUtils.deleteQuietly(new File(fileName));
    }


    @Test(groups = "functional", enabled = false)
    public void testConcurrentFlow() throws Exception {
        ExecutorService executors = Executors.newFixedThreadPool(2);

        Future<Boolean> future1 = executors.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String avroDir = "/user/build/input1";
                prepareDatedTestData(avroDir, "domain1.avro");
                String sourcePath = avroDir + "/*.avro";
                String outputPath = "/user/build/output1";
                String dataFlowBean = "mostRecentDataFlow";
                HdfsUtils.rmdir(yarnConfiguration, outputPath);
                testDataFlowService.executeFunctionalDataflow(sourcePath, outputPath, dataFlowBean, "PropData");
                return true;
            }
        });

        Future<Boolean> future2 = executors.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String avroDir = "/user/build/input2";
                prepareDatedTestData(avroDir, "domain2.avro");
                String sourcePath = avroDir + "/*.avro";
                String outputPath = "/user/build/output2";
                String dataFlowBean = "mostRecentDataFlow";
                HdfsUtils.rmdir(yarnConfiguration, outputPath);
                testDataFlowService.executeFunctionalDataflow(sourcePath, outputPath, dataFlowBean, "PropData2");
                return true;
            }
        });

        future1.get();
        future2.get();

    }

    @Test(groups = "functional", enabled = false)
    public void testMatchFeaturePivoted() throws Exception {
        ExecutorService executors = Executors.newFixedThreadPool(2);

        Future<Boolean> future1 = executors.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String source1Path = "/user/build/TestDomain/*.avro";
                String source2Path = hdfsSourceEntityMgr.getCurrentSnapshotDir(PivotedSource.FEATURE_PIVOTED) + "/*.avro";
                String outputPath = "/user/build/TestDomainFeaturePivoted1";
                String dataFlowBean = "innerJoinDataFlowBuilder";
                HdfsUtils.rmdir(yarnConfiguration, outputPath);
                testDataFlowService.executeJoinDataflow(source1Path, source2Path, outputPath, dataFlowBean, "PropData");
                // uploadAvroToCollectionDB(outputPath, "TestDomains_Matched");
                return true;
            }
        });

        Future<Boolean> future2 = executors.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String source1Path = "/user/build/TestDomain/*.avro";
                String source2Path = hdfsSourceEntityMgr.getCurrentSnapshotDir(PivotedSource.FEATURE_PIVOTED) + "/*.avro";
                String outputPath = "/user/build/TestDomainFeaturePivoted2";
                String dataFlowBean = "innerJoinDataFlowBuilder";
                HdfsUtils.rmdir(yarnConfiguration, outputPath);
                testDataFlowService.executeJoinDataflow(source1Path, source2Path, outputPath, dataFlowBean, "PropData2");
                // uploadAvroToCollectionDB(outputPath, "TestDomains_Matched");
                return true;
            }
        });

        future1.get();
        future2.get();

    }

}
