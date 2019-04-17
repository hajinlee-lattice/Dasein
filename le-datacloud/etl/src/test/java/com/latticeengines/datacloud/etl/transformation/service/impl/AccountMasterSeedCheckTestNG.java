package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.datacloud.dataflow.transformation.AccountMasterSeedChkFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccountMasterSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterSeedCheckTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    GeneralSource source = new GeneralSource("ChkAccountMasterSeed");
    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");

    private static final Logger log = LoggerFactory.getLogger(AccountMasterSeedCheckTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    Object[][] data = new Object[][] { { 1001L, "netapp.com", "DUNS11", "Mumbai", "RTS" },
            { 1002L, "netapp.com", "DUNS13", "San Jose", "RTS" }, { 1006L, "netapp.com", "DUNS11", "San Jose", "HG" },
            { 1003L, "oracle.com", "DUNS14", "Milpitas", "HG" }, { 1007L, "oracle.com", "DUNS14", "San Mateo", "" },
            { 1004L, null, "DUNS16", "Los Angeles", "DnB" }, { 1008L, null, "DUNS17", "Foster City", null },
            { 1008L, null, null, "Burlingame", "Other" } };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("LatticeAccountId", Long.class));
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("City", String.class));
        schema.add(Pair.of("DomainSource", String.class));
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
            configuration.setName("AccontMasterSeedChks");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(AccountMasterSeedChkFlow.TRANSFORMER_NAME);
            String confParamStr1 = getAccountMasterSeedConfig();
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

    private String getAccountMasterSeedConfig() throws JsonProcessingException {
        AccountMasterSeedChkConfig conf = new AccountMasterSeedChkConfig();
        conf.setExceededCountThreshold(10);
        conf.setLessThanThresholdFlag(false);
        conf.setDomain("Domain");
        conf.setDuns("DUNS");
        List<String> grpFields = new ArrayList<String>();
        grpFields.add("Domain");
        grpFields.add("DUNS");
        conf.setGroupByFields(grpFields);
        List<String> idFields = new ArrayList<String>();
        idFields.add("Domain");
        idFields.add("DUNS");
        conf.setId(idFields);
        conf.setCheckField("DUNS");
        conf.setDomainSource("DomainSource");
        Object[] fieldsArray = new Object[] { "DnB", "RTS", "HG", "Orb", "Manual" };
        conf.setKey("LatticeAccountId");
        List<Object> expectedFieldValues = Arrays.asList(fieldsArray);
        conf.setExpectedCoverageFields(expectedFieldValues);
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
            { "OutOfCoverageValForRow", null, "oracle.com,DUNS14", "DomainSource", "",
                    "Row with id [oracle.com,DUNS14] for field [DomainSource] has value [] and it doesnt cover expected group values." }, //
            { "OutOfCoverageValForRow", null, "DUNS17", "DomainSource", null,
                    "Row with id [DUNS17] for field [DomainSource] has value [null] and it doesnt cover expected group values." }, //
            { "OutOfCoverageValForRow", null, "", "DomainSource", "Other",
                    "Row with id [] for field [DomainSource] has value [Other] and it doesnt cover expected group values." }, //
            { "IncompleteCoverageForCol", null, null, "DomainSource", "Orb,Manual",
                    "No record found for field [DomainSource] with group value [Orb,Manual]." }, //
            { "EmptyField", "null,DUNS16", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "EmptyField", "null,DUNS17", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "EmptyField", "null,null", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "BelowExpectedCount", null, null, "__COUNT__", "8", "Total count of records below [10]" }, //
            { "DuplicatedValue", null, "1008", "[LatticeAccountId]", "2",
                    "Found duplicated value [1008] in the field [[LatticeAccountId]]." }, //
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
            log.info("record : " + record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 11);
    }

}
