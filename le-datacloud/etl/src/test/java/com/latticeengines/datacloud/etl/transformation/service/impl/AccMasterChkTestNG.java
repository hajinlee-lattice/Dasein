package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.AccMasterChkFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMasterChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccMasterChkTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    GeneralSource source = new GeneralSource("ChkAccMaster");
    GeneralSource baseSource1 = new GeneralSource("AccountMaster");
    private static final Logger log = LoggerFactory.getLogger(AccMasterChkTestNG.class);

    Object[][] data = new Object[][] { { 1001L, "netapp.com", "DUNS11" }, { 1001L, "netapp.com", "DUNS13" },
            { null, "netapp.com", "DUNS11" }, { 1004L, "oracle.com", "DUNS14" }, { 1005L, "oracle.com", "DUNS14" },
            { 1007L, null, "DUNS16" }, { 1007L, null, "DUNS16" },
            { 1008L, null, null } };

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("LatticeID", Long.class));
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("LDC_DUNS", String.class));

        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data);
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
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AccMasterChks");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(AccMasterChkFlow.TRANSFORMER_NAME);
            String confParamStr1 = getAccMasterConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(source.getSourceName());

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getAccMasterConfig() throws JsonProcessingException {
        AccMasterChkConfig conf = new AccMasterChkConfig();
        conf.setKey("LatticeID");
        conf.setDomain("Domain");
        conf.setDuns("LDC_DUNS");
        List<String> idFields = new ArrayList<String>();
        idFields.add("LatticeID");
        conf.setId(idFields);
        List<String> fieldList = new ArrayList<String>();
        fieldList.add("Domain");
        fieldList.add("LDC_DUNS");
        conf.setGroupByFields(fieldList);
        conf.setExceededCountThreshold(10);
        conf.setLessThanThresholdFlag(false);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[][] expectedDataValues = new Object[][] { //
            { "DuplicatedValue", null, "netapp.com,DUNS11", "[Domain, LDC_DUNS]", "2",
                    "Found duplicated value [netapp.com,DUNS11] in the field [[Domain, LDC_DUNS]]." }, //
            { "DuplicatedValue", null, "oracle.com,DUNS14", "[Domain, LDC_DUNS]", "2",
                    "Found duplicated value [oracle.com,DUNS14] in the field [[Domain, LDC_DUNS]]." }, //
            { "DuplicatedValue", null, "null,DUNS16", "[Domain, LDC_DUNS]", "2",
                    "Found duplicated value [null,DUNS16] in the field [[Domain, LDC_DUNS]]." }, //
            { "DuplicatedValue", null, "1001", "[LatticeID]", "2",
                    "Found duplicated value [1001] in the field [[LatticeID]]." }, //
            { "BelowExpectedCount", null, null, "__COUNT__", "8", "Total count of records below [10]" }, //
            { "EmptyField", null, null, "LatticeID", null, "Field [LatticeID] should not be null or empty." }, //
            { "DuplicatedValue", null, "1007", "[LatticeID]", "2",
                    "Found duplicated value [1007] in the field [[LatticeID]]." } };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[0]) + (String.valueOf(data[1]))
                    + (String.valueOf(data[2]) + (String.valueOf(data[3]))), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String chkCode = String.valueOf(record.get(DataCloudConstants.CHK_ATTR_CHK_CODE));
            String chkField = String.valueOf(record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD));
            String groupId = String.valueOf(record.get(DataCloudConstants.CHK_ATTR_GROUP_ID));
            String rowId = String.valueOf(record.get(DataCloudConstants.CHK_ATTR_ROW_ID));
            Object[] expected = expectedData.get(chkCode + rowId + groupId + chkField);
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_CHK_CODE), expected[0]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_ROW_ID), expected[1]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_GROUP_ID), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD), expected[3]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE), expected[4]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.CHK_ATTR_CHK_MSG), expected[5]));
            log.info("record : " + record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 7);
    }

}
