package com.latticeengines.datacloud.etl.transformation.service.impl.atlas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.atlas.ContactNameConcatenate;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.TransformationServiceImplTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ContactNameConcatenaterTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ContactNameConcatenaterTestNG.class);
    private GeneralSource baseSource = new GeneralSource("AccountMaster");
    private GeneralSource source = new GeneralSource("ContactNameConcatenated");
    private static final String VERSION = "2017-12-22_00-00-00_UTC";
    private boolean isNameProvided = false;

    private final String RESULT_FIELD_NAME = "FullName";
    private String[] retainedFields = {"CompanyName", "City", "State", "Country", "Email"};
    private String[] retainedFieldsWithName = {"CompanyName", "City", "State", "Country", "Email", RESULT_FIELD_NAME};
    private String[] concatenateFields = {"FirstName", "LastName"};

    private Object[][] inputData = new Object[][] {
            {"Google", "MountainView", "CA", "USA", "google.com", "google_fname", "google_lname"},
            {"Twitter", "San Francisco", "CA", "USA", "twitter.com", "twitter_fname", "twitter_lname"},
            {"Amazon", "Seattle", "WA", "USA", "amazon.com", "amazon_fname", "amazon_lname"},
            {"Apple", "Cupertino", "CA", "USA", "apple.com", "", "apple_lname"},
            {"Microsoft", "Seattle", "WA", "USA", "microsoft.com", "microsoft_fname", null},
            {"LinkedIn", "Sunnyvale", "CA", "USA", "linkedin.com", null, null}
    };

    private Object[][] expectedData = new Object[][] {
            {"Google", "MountainView", "CA", "USA", "google.com", "google_fname google_lname"},
            {"Twitter", "San Francisco", "CA", "USA", "twitter.com", "twitter_fname twitter_lname"},
            {"Amazon", "Seattle", "WA", "USA", "amazon.com", "amazon_fname amazon_lname"},
            {"Apple", "Cupertino", "CA", "USA", "apple.com", "apple_lname"},
            {"Microsoft", "Seattle", "WA", "USA", "microsoft.com", "microsoft_fname"},
            {"LinkedIn", "Sunnyvale", "CA", "USA", "linkedin.com"}
    };

    private Object[][] inputDataWithName = new Object[][] {
            {"Google", "MountainView", "CA", "USA", "google.com", "google_full_name", "google_fname", "google_lname"},
            {"Twitter", "San Francisco", "CA", "USA", "twitter.com", "", "twitter_fname", "twitter_lname"},
            {"Amazon", "Seattle", "WA", "USA", "amazon.com", null, "amazon_fname", "amazon_lname"}
    };

    private Object[][] expectedDataWithName = new Object[][] {
            {"Google", "MountainView", "CA", "USA", "google.com", "google_full_name"},
            {"Twitter", "San Francisco", "CA", "USA", "twitter.com", ""},
            {"Amazon", "Seattle", "WA", "USA", "amazon.com", "amazon_fname amazon_lname"}
    };

    @Test(groups = "functional")
    public void testTransformation() {
        isNameProvided = false;
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Test(groups = "functinoal")
    public void testTransformationWithNameColumn() {
        isNameProvided = true;
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("ContactNameConcatenater");
        configuration.setVersion(targetVersion);

        TransformationStepConfig singleStep = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(baseSource.getSourceName());
        singleStep.setBaseSources(baseSources);
        singleStep.setTransformer(ContactNameConcatenate.TRANSFORMER_NAME);
        singleStep.setTargetSource(source.getSourceName());
        String configString = getContactNameConcatenateConfig();
        singleStep.setConfiguration(configString);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(singleStep);
        configuration.setSteps(steps);

        return configuration;
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<String, Object[]> expectedMap = new HashMap<>();
        if (isNameProvided) {
            for (Object[] data : expectedDataWithName) {
                expectedMap.put((String) data[0], data);
            }

            int rowNum = 0;
            while (records.hasNext()) {
                GenericRecord record = records.next();
                log.info(record.toString());
                Object[] expectedResult = expectedMap.get(record.get(retainedFieldsWithName[0]).toString());
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[0]), expectedResult[0]));
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[1]), expectedResult[1]));
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[2]), expectedResult[2]));
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[3]), expectedResult[3]));
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[4]), expectedResult[4]));
                Assert.assertTrue(isObjEquals(record.get(retainedFieldsWithName[5]), expectedResult[5]));
                rowNum++;
            }
            Assert.assertEquals(rowNum, 3);
        } else {
            for (Object[] data : expectedData) {
                expectedMap.put((String) data[0], data);
            }

            int rowNum = 0;
            while (records.hasNext()) {
                GenericRecord record = records.next();
                log.info(record.toString());
                Object[] expectedResult = expectedMap.get(record.get(retainedFields[0]).toString());
                Assert.assertTrue(isObjEquals(record.get(retainedFields[0]), expectedResult[0]));
                Assert.assertTrue(isObjEquals(record.get(retainedFields[1]), expectedResult[1]));
                Assert.assertTrue(isObjEquals(record.get(retainedFields[2]), expectedResult[2]));
                Assert.assertTrue(isObjEquals(record.get(retainedFields[3]), expectedResult[3]));
                Assert.assertTrue(isObjEquals(record.get(retainedFields[4]), expectedResult[4]));
                if (expectedResult.length > 5) {
                    Assert.assertTrue(isObjEquals(record.get(RESULT_FIELD_NAME), expectedResult[5]));
                } else {
                    Assert.assertNull(record.get(RESULT_FIELD_NAME));
                }
                rowNum++;
            }
            Assert.assertEquals(rowNum, 6);
        }
    }

    private String getContactNameConcatenateConfig() {
        ContactNameConcatenateConfig conf = new ContactNameConcatenateConfig();
        conf.setConcatenateFields(concatenateFields);
        conf.setResultField(RESULT_FIELD_NAME);
        return JsonUtils.serialize(conf);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        if (isNameProvided) {
            for (String retainedField : retainedFieldsWithName) {
                columns.add(Pair.of(retainedField, String.class));
            }
        } else {
            for (String retainedField : retainedFields) {
                columns.add(Pair.of(retainedField, String.class));
            }
        }

        for (String concatenateField : concatenateFields) {
            columns.add(Pair.of(concatenateField, String.class));
        }

        if (isNameProvided) {
            uploadBaseSourceData(baseSource.getSourceName(), VERSION, columns, inputDataWithName);
        } else {
            uploadBaseSourceData(baseSource.getSourceName(), VERSION, columns, inputData);
        }

        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(baseSource, VERSION).toString();
            List<String> src2Files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(src2Files.get(0)));
            String avscPath = hdfsPathBuilder.constructSchemaFile(baseSource.getSourceName(), VERSION).toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } catch (IOException exc) {
            throw new RuntimeException("Failed to create schema file for AccountMaster ", exc);
        }
    }
}
