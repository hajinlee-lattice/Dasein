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
import com.latticeengines.datacloud.dataflow.transformation.AccMastrIdChkFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMastrIdChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterIdChkTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    GeneralSource source = new GeneralSource("ChkAccMasterIdChkTestNG");
    GeneralSource baseSource1 = new GeneralSource("AccountMasterID");

    private static final Logger log = LoggerFactory.getLogger(AccountMasterIdChkTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    Object[][] data = new Object[][] { { 1001L, 2001L, "netapp.com", "DUNS11", "INACTIVE" },
            { 1002L, 2002L, "netapp.com", "DUNS13", "ACTIVE" }, { 1003L, 2003L, "netapp.com", "DUNS11", "ACTIVE" },
            { 1004L, 2004L, "oracle.com", "DUNS14", "ACTIVE" }, { 1005L, 2005L, "oracle.com", "DUNS14", "ACTIVE" },
            { 1008L, 2006L, null, "DUNS16", "INACTIVE" }, { 1008L, 2008L, null, "DUNS16", "ACTIVE" },
            { 1008L, 2008L, null, null, "ACTIVE" } };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("RedirectFromId", Long.class));
        schema.add(Pair.of("LatticeID", Long.class));
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("Status", String.class));
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
            configuration.setName("AccMastrIdChks");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(AccMastrIdChkFlow.TRANSFORMER_NAME);
            String confParamStr1 = getAccMastrIdChkConfig();
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

    private String getAccMastrIdChkConfig() throws JsonProcessingException {
        AccMastrIdChkConfig conf = new AccMastrIdChkConfig();
        conf.setStatus("Status");
        conf.setDomain("Domain");
        conf.setDuns("DUNS");
        conf.setKey("LatticeID");
        conf.setRedirectFromId("RedirectFromId");
        conf.setCheckDupWithStatus(true);
        List<String> fieldList = new ArrayList<String>();
        fieldList.add("LatticeID");
        conf.setGroupByFields(fieldList);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[][] expectedDataValues = new Object[][] { //
            { "DuplicatedValue", null, "netapp.com,DUNS11", "[Domain, DUNS]", "2",
                    "Found duplicated value [netapp.com,DUNS11] in the field [[Domain, DUNS]]." }, //
            { "DuplicatedValue", null, "oracle.com,DUNS14", "[Domain, DUNS]", "2",
                    "Found duplicated value [oracle.com,DUNS14] in the field [[Domain, DUNS]]." }, //
            { "DuplicatedValue", null, "null,DUNS16", "[Domain, DUNS]", "2",
                    "Found duplicated value [null,DUNS16] in the field [[Domain, DUNS]]." }, //
            { "DuplicatedValue", null, "1008", "[RedirectFromId]", "3",
                    "Found duplicated value [1008] in the field [[RedirectFromId]]." }, //
            { "DuplicatedValuesWithStatus", null, "2008", "[LatticeID]", "2",
                    "Found duplicated value [2008] in the field [[LatticeID]]." }, //
    };

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
            rowCount++;
            log.info("record : " + record);
        }
        Assert.assertEquals(rowCount, 5);
    }
}
