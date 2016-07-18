package com.latticeengines.scoring.yarn.runtime;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

import edu.emory.mathcs.backport.java.util.Arrays;

public class ScoringProcessorTestNG extends ScoringFunctionalTestNGBase {

    private ScoringProcessor bulkScoringProcessor;

    @Autowired
    private Configuration yarnConfiguration;

    private String tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();

    private String dir;

    private String filePath;

    private String modelGuidString;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        bulkScoringProcessor = new ScoringProcessor();
        bulkScoringProcessor.setConfiguration(yarnConfiguration);
        dir = customerBaseDir + "/test_customer/scoring/data/some_random_directory";
        modelGuidString = "modelGuid";
        HdfsUtils.rmdir(yarnConfiguration, dir);
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
        URL uploadedAvro = ClassLoader.getSystemResource("com/latticeengines/scoring/data/upload-file.avro"); //
        HdfsUtils.mkdir(yarnConfiguration, dir);
        filePath = dir + "/upload-file.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, uploadedAvro.getFile(), filePath);
    }

    @AfterMethod(groups = "functional")
    public void AfterMethod() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, dir);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void testConvertAvroToBulkScoreRequest() throws IllegalArgumentException, Exception {
        RTSBulkScoringConfiguration rtsBulkScoringConfig = new RTSBulkScoringConfiguration();
        rtsBulkScoringConfig.setModelGuids(Arrays.asList(new String[] { modelGuidString }));

        List<BulkRecordScoreRequest> scoreRequestList = bulkScoringProcessor.convertAvroToBulkScoreRequest(dir,
                rtsBulkScoringConfig);
        Assert.assertEquals(scoreRequestList.size(), 107);
        BulkRecordScoreRequest bulkRecordScoreRequest = scoreRequestList.get(0);
        Assert.assertNotNull(bulkRecordScoreRequest.getRecords());
        Assert.assertEquals(bulkRecordScoreRequest.getSource(), ScoringProcessor.RECORD_SOURCE);

        Record record = bulkRecordScoreRequest.getRecords().get(0);
        Assert.assertEquals(record.getIdType(), ScoringProcessor.DEFAULT_ID_TYPE);
        Assert.assertEquals(bulkRecordScoreRequest.getRecords().size(), 200);
        Assert.assertEquals(scoreRequestList.get(106).getRecords().size(), 62);
        Assert.assertEquals(record.getModelAttributeValuesMap().size(), 1);
        Assert.assertTrue(record.getModelAttributeValuesMap().containsKey(modelGuidString));
        Assert.assertEquals(record.getRule(), ScoringProcessor.RECORD_RULE);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvro() throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponse();
        bulkScoringProcessor.convertBulkScoreResponseToAvro(recordScoreResponseList, dir + "/score", null);
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dir + "/score");
        Assert.assertNotNull(files);
        Assert.assertEquals(files.size(), 1);
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, files.get(0));
        Assert.assertNotNull(contents);
        List<GenericRecord> list = AvroUtils.getData(new Configuration(), new Path(files.get(0)));
        Assert.assertEquals(list.size(), 3);
        for (GenericRecord ele : list) {
            System.out.println(ele.toString());
        }
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithCorrectAttributeMap() throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponseWithEnrichmentAttributeMap();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = generateCorrectEnrichmentAttributeMap();
        bulkScoringProcessor.convertBulkScoreResponseToAvro(recordScoreResponseList, dir + "/score",
                leadEnrichmentAttributeMap);
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dir + "/score");
        Assert.assertNotNull(files);
        Assert.assertEquals(files.size(), 1);
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, files.get(0));
        Assert.assertNotNull(contents);
        List<GenericRecord> list = AvroUtils.getData(new Configuration(), new Path(files.get(0)));
        Assert.assertEquals(list.size(), 3);
        for (GenericRecord ele : list) {
            Assert.assertNotNull(ele.get("attr1"));
            Assert.assertNotNull(ele.get("attr2"));
        }
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithIncorrectAttributeMap() throws IllegalArgumentException,
            Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponseWithEnrichmentAttributeMap();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = generateIncorrectEnrichmentAttributeMap();
        try {
            bulkScoringProcessor.convertBulkScoreResponseToAvro(recordScoreResponseList, dir + "/score",
                    leadEnrichmentAttributeMap);
            Assert.fail("Should have thrown exception");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20039);
        }
    }

    private Map<String, Type> generateCorrectEnrichmentAttributeMap() {
        Map<String, Schema.Type> attributeMap = new HashMap<String, Schema.Type>();
        attributeMap.put("attr1", Schema.Type.STRING);
        attributeMap.put("attr2", Schema.Type.BOOLEAN);
        return attributeMap;
    }

    private Map<String, Type> generateIncorrectEnrichmentAttributeMap() {
        Map<String, Schema.Type> attributeMap = generateCorrectEnrichmentAttributeMap();
        attributeMap.remove("attr1");
        return attributeMap;
    }

    private List<RecordScoreResponse> generateRecordScoreResponseWithEnrichmentAttributeMap() {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponse();
        Map<String, Object> enrichmentAttributes1 = new HashMap<String, Object>();
        enrichmentAttributes1.put("attr1", "str1");
        enrichmentAttributes1.put("attr2", Boolean.TRUE);
        Map<String, Object> enrichmentAttributes2 = new HashMap<String, Object>();
        enrichmentAttributes2.put("attr1", "str2");
        enrichmentAttributes2.put("attr2", Boolean.FALSE);
        recordScoreResponseList.get(0).setEnrichmentAttributeValues(enrichmentAttributes1);
        recordScoreResponseList.get(1).setEnrichmentAttributeValues(enrichmentAttributes2);
        return recordScoreResponseList;
    }

    private List<RecordScoreResponse> generateRecordScoreResponse() {
        List<RecordScoreResponse> recordScoreResponseList = new ArrayList<RecordScoreResponse>();
        RecordScoreResponse record1 = new RecordScoreResponse();
        RecordScoreResponse record2 = new RecordScoreResponse();
        record1.setId("1");
        record2.setId("2");
        List<ScoreModelTuple> scores1 = new ArrayList<ScoreModelTuple>();
        ScoreModelTuple tuple1 = new ScoreModelTuple();
        tuple1.setModelId("model1");
        tuple1.setScore(99.0);
        ScoreModelTuple tuple2 = new ScoreModelTuple();
        tuple2.setModelId("model2");
        tuple2.setScore(98.0);
        scores1.add(tuple1);
        scores1.add(tuple2);
        record1.setScores(scores1);
        List<ScoreModelTuple> scores2 = new ArrayList<ScoreModelTuple>();
        ScoreModelTuple tuple3 = new ScoreModelTuple();
        tuple3.setModelId("model1");
        tuple3.setScore(8.0);
        scores2.add(tuple3);
        record1.setScores(scores1);
        record2.setScores(scores2);
        recordScoreResponseList.add(record1);
        recordScoreResponseList.add(record2);
        return recordScoreResponseList;
    }

    @AfterMethod(groups = "functional")
    public void cleanupHdfs() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, dir);
    }
}
