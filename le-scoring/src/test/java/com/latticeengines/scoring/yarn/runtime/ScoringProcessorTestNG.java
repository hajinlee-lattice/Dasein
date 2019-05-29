package com.latticeengines.scoring.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringProcessorTestNG extends ScoringFunctionalTestNGBase {

    private ScoringProcessor bulkScoringProcessor;

    @Autowired
    private Configuration yarnConfiguration;

    private String dir;

    private URL uploadedAvro;

    private String filePath;

    private String modelGuidString;

    private RTSBulkScoringConfiguration rtsBulkScoringConfig;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        uploadedAvro = ClassLoader
                .getSystemResource("com/latticeengines/scoring/data/allTest_with_lattice_accountId.avro");
        dir = customerBaseDir + "/test_customer/scoring/data/some_random_directory";
        modelGuidString = "modelGuid";
        HdfsUtils.rmdir(yarnConfiguration, dir);
        rtsBulkScoringConfig = new RTSBulkScoringConfiguration();
        rtsBulkScoringConfig.setModelGuids(Arrays.asList(modelGuidString));
        rtsBulkScoringConfig.setModelType(ModelType.PYTHONMODEL.getModelType());
        rtsBulkScoringConfig.setEnableLeadEnrichment(true);
        rtsBulkScoringConfig.setScoreTestFile(true);
        bulkScoringProcessor = new ScoringProcessor(rtsBulkScoringConfig);
        bulkScoringProcessor.setConfiguration(yarnConfiguration);
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
        HdfsUtils.mkdir(yarnConfiguration, dir);
        filePath = dir + "/allTest_with_lattice_accountId.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, uploadedAvro.getFile(), filePath);
    }

    @AfterMethod(groups = "functional")
    public void AfterMethod() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, dir);
        FileUtils.deleteQuietly(new File(ScoringDaemonService.IMPORT_ERROR_FILE_NAME));
    }

    @Test(groups = "functional")
    public void testAvroUtils() throws IOException {
        // upload anther avro file
        String anotherFilePath = dir + "/allTest_with_lattice_accountId_1.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, uploadedAvro.getFile(), anotherFilePath);

        int count = 0;
        try (AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, dir + "/*.avro")) {
            while (iterator.hasNext()) {
                count++;
                iterator.next();
            }
        }
        Assert.assertEquals(count, 852);
    }

    @Test(groups = "functional")
    public void testConvertAvroToBulkScoreRequest() throws IllegalArgumentException, Exception {
        List<BulkRecordScoreRequest> scoreRequestList = new ArrayList<>();
        Iterator<GenericRecord> iterator = bulkScoringProcessor.instantiateIteratorForBulkScoreRequest(dir);
        BulkRecordScoreRequest scoreRequest = null;
        do {
            scoreRequest = bulkScoringProcessor.getBulkScoreRequest(iterator, rtsBulkScoringConfig);
            if (scoreRequest == null) {
                break;
            }
            scoreRequestList.add(scoreRequest);
        } while (scoreRequest != null);

        Assert.assertEquals(scoreRequestList.size(), 5);
        BulkRecordScoreRequest bulkRecordScoreRequest = scoreRequestList.get(0);
        Assert.assertNotNull(bulkRecordScoreRequest.getRecords());
        Assert.assertEquals(bulkRecordScoreRequest.getSource(), ScoringProcessor.RECORD_SOURCE);

        Record record = bulkRecordScoreRequest.getRecords().get(0);
        Assert.assertEquals(record.getIdType(), ScoringProcessor.DEFAULT_ID_TYPE);
        Assert.assertEquals(bulkRecordScoreRequest.getRecords().size(), 100);
        Assert.assertEquals(scoreRequestList.get(4).getRecords().size(), 26);
        Assert.assertEquals(record.getModelAttributeValuesMap().size(), 1);
        Table metadataTable = MetadataConverter.getTable(yarnConfiguration, dir);
        Assert.assertEquals(record.getModelAttributeValuesMap().get(modelGuidString).size(),
                metadataTable.getAttributes().size());
        Assert.assertTrue(record.getModelAttributeValuesMap().containsKey(modelGuidString));
        Assert.assertEquals(record.getRule(), ScoringProcessor.RECORD_RULE);
    }

    @Test(groups = "functional")
    public void testConvertMultipleAvroToBulkScoreRequest() throws IllegalArgumentException, Exception {
        // upload anther avro file
        String anotherFilePath = dir + "/allTest_with_lattice_accountId_1.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, uploadedAvro.getFile(), anotherFilePath);

        List<BulkRecordScoreRequest> scoreRequestList = new ArrayList<>();
        Iterator<GenericRecord> iterator = bulkScoringProcessor.instantiateIteratorForBulkScoreRequest(dir);
        BulkRecordScoreRequest scoreRequest = null;
        do {
            scoreRequest = bulkScoringProcessor.getBulkScoreRequest(iterator, rtsBulkScoringConfig);
            if (scoreRequest == null) {
                break;
            }
            scoreRequestList.add(scoreRequest);
        } while (scoreRequest != null);

        Assert.assertEquals(scoreRequestList.size(), 9);
        BulkRecordScoreRequest bulkRecordScoreRequest = scoreRequestList.get(0);
        Assert.assertNotNull(bulkRecordScoreRequest.getRecords());
        Assert.assertEquals(bulkRecordScoreRequest.getSource(), ScoringProcessor.RECORD_SOURCE);

        Record record = bulkRecordScoreRequest.getRecords().get(0);
        Assert.assertEquals(record.getIdType(), ScoringProcessor.DEFAULT_ID_TYPE);
        Assert.assertEquals(bulkRecordScoreRequest.getRecords().size(), 100);
        Assert.assertEquals(scoreRequestList.get(8).getRecords().size(), 52);
    }

    @Test(groups = "functional", dependsOnMethods = "testConvertAvroToBulkScoreRequest")
    public void testConvertAvroToBulkScoreRequestWithScoreTestFileOff() throws IllegalArgumentException, Exception {
        List<BulkRecordScoreRequest> scoreRequestList = new ArrayList<>();
        BulkRecordScoreRequest scoreRequest = null;
        rtsBulkScoringConfig.setScoreTestFile(false);
        Table metadataTable = MetadataConverter.getTable(yarnConfiguration, dir);

        Set<String> internalPlusMustHaveAttributeNames = new HashSet<>();
        internalPlusMustHaveAttributeNames.add(InterfaceName.LatticeAccountId.toString());
        internalPlusMustHaveAttributeNames.add(InterfaceName.InternalId.toString());
        for (Attribute attribute : metadataTable.getAttributes()) {
            if (attribute.isInternalPredictor()) {
                System.out.println(String.format("attribute %s is internal.", metadataTable.getAttributes()));
                internalPlusMustHaveAttributeNames.add(attribute.getName());
            }
        }
        System.out.println("internalPlusMustHaveAttributeNames is " + internalPlusMustHaveAttributeNames);

        Iterator<GenericRecord> iterator = bulkScoringProcessor.instantiateIteratorForBulkScoreRequest(dir);
        rtsBulkScoringConfig.setMetadataTable(metadataTable);

        scoreRequest = bulkScoringProcessor.getBulkScoreRequest(iterator, rtsBulkScoringConfig);
        scoreRequestList.add(scoreRequest);

        Assert.assertEquals(scoreRequestList.size(), 1);
        BulkRecordScoreRequest bulkRecordScoreRequest = scoreRequestList.get(0);
        Assert.assertNotNull(bulkRecordScoreRequest.getRecords());
        Assert.assertEquals(bulkRecordScoreRequest.getSource(), ScoringProcessor.RECORD_SOURCE);

        Record record = bulkRecordScoreRequest.getRecords().get(0);
        Assert.assertEquals(record.getIdType(), ScoringProcessor.DEFAULT_ID_TYPE);
        Assert.assertEquals(bulkRecordScoreRequest.getRecords().size(), 100);
        Assert.assertEquals(record.getModelAttributeValuesMap().size(), 1);
        Assert.assertTrue(record.getModelAttributeValuesMap().containsKey(modelGuidString));
        Assert.assertNotNull(record.getModelAttributeValuesMap().get(modelGuidString));
        Map<String, Object> attributeValues = record.getModelAttributeValuesMap().get(modelGuidString);
        Assert.assertTrue(attributeValues.containsKey(InterfaceName.LatticeAccountId.toString()));
        Assert.assertTrue(attributeValues.containsKey(InterfaceName.InternalId.toString()));
        Assert.assertEquals(record.getRule(), ScoringProcessor.RECORD_RULE);
        System.out.println("record is " + record);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvro() throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponse();

        Map<String, Schema.Type> leadEnrichmentAttributeMap = null;
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = null;
        generateScoreResponseAvroAndCopyToHdfs(recordScoreResponseList, leadEnrichmentAttributeMap,
                leadEnrichmentAttributeDisplayNameMap, dir + "/score");
        checkScoreAndErrorFiles(false);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithCorrectAttributeMap() throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponseWithEnrichmentAttributeMap();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = new HashMap<>();
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = new HashMap<>();
        generateCorrectEnrichmentAttributeMap(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
        generateScoreResponseAvroAndCopyToHdfs(recordScoreResponseList, leadEnrichmentAttributeMap,
                leadEnrichmentAttributeDisplayNameMap, dir + "/score");
        checkScoreAndErrorFiles(true);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithIncompleteAttributeMap()
            throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponseWithEnrichmentAttributeMap();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = new HashMap<>();
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = new HashMap<>();
        generateIncorrectEnrichmentAttributeMap(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
        try {
            generateScoreResponseAvroAndCopyToHdfs(recordScoreResponseList, leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap, dir + "/score");
            Assert.assertTrue(true, "Should not have thrown exception");
        } catch (LedpException e) {
            Assert.fail("Should NOT have thrown exception");
        }
        checkScoreAndErrorFiles(false);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithNullAttributeMap() throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponseWithEnrichmentAttributeMap();
        try {
            generateScoreResponseAvroAndCopyToHdfs(recordScoreResponseList, null, null, dir + "/score");
            Assert.assertTrue(true, "Should not have thrown exception");
        } catch (LedpException e) {
            Assert.fail("Should NOT have thrown exception");
        }
        checkScoreAndErrorFiles(false);
    }

    @Test(groups = "functional")
    public void testConvertBulkScoreResponseToAvroWithAttributeMapButNullAttributs()
            throws IllegalArgumentException, Exception {
        List<RecordScoreResponse> recordScoreResponseList = generateRecordScoreResponse();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = new HashMap<>();
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = new HashMap<>();
        generateIncorrectEnrichmentAttributeMap(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
        try {
            generateScoreResponseAvroAndCopyToHdfs(recordScoreResponseList, leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap, dir + "/score");
            Assert.assertTrue(true, "Should not have thrown exception");
        } catch (LedpException e) {
            Assert.fail("Should NOT have thrown exception");
        }
        checkScoreAndErrorFiles(false);
    }

    private void checkScoreAndErrorFiles(boolean withAttributes) throws IllegalArgumentException, Exception {
        List<String> avrofiles = HdfsUtils.getFilesForDir(yarnConfiguration, dir + "/score", ".*.avro$");
        List<String> csvfiles = HdfsUtils.getFilesForDir(yarnConfiguration, dir + "/score", ".*.csv$");
        Assert.assertNotNull(avrofiles);
        Assert.assertNotNull(csvfiles);
        Assert.assertEquals(avrofiles.size(), 1);
        Assert.assertEquals(csvfiles.size(), 1);
        checkScoreAvro(avrofiles.get(0), withAttributes);
        checkErrorCSV(csvfiles.get(0));
    }

    private void checkScoreAvro(String path, boolean withAttributes) throws IllegalArgumentException, Exception {
        String scoreContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        Assert.assertNotNull(scoreContents);
        List<GenericRecord> list = AvroUtils.getData(yarnConfiguration, new Path(path));
        Assert.assertEquals(list.size(), 4);
        for (GenericRecord ele : list) {
            System.out.println(ele.toString());
        }
        if (withAttributes) {
            GenericRecord record = list.get(0);
            Schema schema = record.getSchema();
            List<Schema.Field> fields = schema.getFields();
            Assert.assertEquals(fields.get(0).name(), InterfaceName.Id.toString());
            Assert.assertEquals(fields.get(1).name(), ScoreResultField.ModelId.displayName);
            Assert.assertEquals(fields.get(2).name(), ScoreResultField.Percentile.displayName);
            Assert.assertEquals(fields.get(3).name(), ScoreResultField.Rating.displayName);
            System.out.println(record.get(0));
            System.out.println(record.get(1));
            System.out.println(record.get(2));
            System.out.println(record.get(3));
            System.out.println(record.get(4));
            for (GenericRecord ele : list) {
                Assert.assertNotNull(ele.get("attr1"));
                Assert.assertNotNull(ele.get("attr2"));
            }
        }
    }

    private void checkErrorCSV(String filePath) throws IOException {
        try (CSVParser parser = new CSVParser(
                new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration, filePath)), LECSVFormat.format)) {
            List<CSVRecord> records = parser.getRecords();
            Assert.assertEquals(records.size(), 1);
            CSVRecord record = records.get(0);
            Assert.assertEquals(record.get("Id"), "2");
            Assert.assertEquals(record.get("ErrorMessage"), "some error occurred");
        }
        FileUtils.deleteQuietly(new File(ScoringDaemonService.IMPORT_ERROR_FILE_NAME));
    }

    private void generateScoreResponseAvroAndCopyToHdfs(List<RecordScoreResponse> recordScoreResponseList,
            Map<String, Schema.Type> leadEnrichmentAttributeMap,
            Map<String, String> leadEnrichmentAttributeDisplayNameMap, String targetDir) throws IOException {
        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        Schema schema = bulkScoringProcessor.createOutputSchema(leadEnrichmentAttributeMap,
                leadEnrichmentAttributeDisplayNameMap);
        try (CSVPrinter csvFilePrinter = bulkScoringProcessor.initErrorCSVFilePrinter("")) {
            try (DataFileWriter<GenericRecord> dataFileWriter = bulkScoringProcessor.createDataFileWriter(schema,
                    fileName)) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                bulkScoringProcessor.appendScoreResponseToAvro(recordScoreResponseList, dataFileWriter, builder,
                        leadEnrichmentAttributeMap, csvFilePrinter);
            }
        }
        bulkScoringProcessor.copyScoreOutputToHdfs(fileName, targetDir);
    }

    private void generateCorrectEnrichmentAttributeMap(Map<String, Type> leadEnrichmentAttributeMap,
            Map<String, String> leadEnrichmentAttributeDisplayNameMap) {
        leadEnrichmentAttributeMap.put("attr1", Schema.Type.STRING);
        leadEnrichmentAttributeMap.put("attr2", Schema.Type.BOOLEAN);
        leadEnrichmentAttributeDisplayNameMap.put("attr1", "Display name of attr1");
        leadEnrichmentAttributeDisplayNameMap.put("attr2", "Display name of attr2");
    }

    private void generateIncorrectEnrichmentAttributeMap(Map<String, Type> leadEnrichmentAttributeMap,
            Map<String, String> leadEnrichmentAttributeDisplayNameMap) {
        generateCorrectEnrichmentAttributeMap(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
        leadEnrichmentAttributeMap.remove("attr1");
        leadEnrichmentAttributeDisplayNameMap.remove("attr1");
        leadEnrichmentAttributeMap.put("attr4", Schema.Type.BOOLEAN);
        leadEnrichmentAttributeDisplayNameMap.put("attr4", "Display name of attr4");
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
        tuple1.setScore(99);
        tuple1.setBucket(BucketName.A_PLUS.toValue());
        ScoreModelTuple tuple2 = new ScoreModelTuple();
        tuple2.setModelId("model2");
        tuple2.setScore(98);
        tuple2.setBucket(BucketName.A.toValue());
        scores1.add(tuple1);
        scores1.add(tuple2);

        List<ScoreModelTuple> scores2 = new ArrayList<ScoreModelTuple>();
        ScoreModelTuple tuple3 = new ScoreModelTuple();
        tuple3.setModelId("model1");
        tuple3.setScore(8);
        tuple3.setBucket(BucketName.D.toValue());
        scores2.add(tuple3);

        ScoreModelTuple tuple4 = new ScoreModelTuple();
        tuple4.setModelId("model2");
        tuple4.setErrorDescription("some error occurred");
        tuple4.setBucket(null);
        scores2.add(tuple4);
        record1.setScores(scores1);
        record2.setScores(scores2);
        recordScoreResponseList.add(record1);
        recordScoreResponseList.add(record2);
        return recordScoreResponseList;
    }

}
