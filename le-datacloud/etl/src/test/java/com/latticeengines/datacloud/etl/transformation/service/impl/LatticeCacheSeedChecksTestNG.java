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
import com.latticeengines.datacloud.dataflow.transformation.LatticeCacheSeedChkFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.LatticeCacheSeedChkConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class LatticeCacheSeedChecksTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("ChkLatticeCacheSeed");
    GeneralSource baseSource1 = new GeneralSource("LatticeCacheSeed");

    private static final Logger log = LoggerFactory.getLogger(LatticeCacheSeedChecksTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    Object[][] data = new Object[][] { { "L01", "netapp.com", "DUNS11", "Mumbai" },
            { "L02", "netapp.com", "DUNS13", "San Jose" }, { "L03", "netapp.com", "DUNS11", "San Jose" },
            { "L04", "oracle.com", "DUNS14", "Milpitas" }, { "L05", "oracle.com", "DUNS14", "San Mateo" },
            { "L06", null, "DUNS16", "Los Angeles" }, { "L07", null, "DUNS16", "Foster City" },
            { "L08", null, null, "Burlingame" } };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("ID", String.class));
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("City", String.class));
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
            configuration.setName("LatticeCacheSeedChks");
            configuration.setVersion(targetVersion);
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(LatticeCacheSeedChkFlow.TRANSFORMER_NAME);
            String confParamStr1 = getLatticeSeedConfig();
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

    private String getLatticeSeedConfig() throws JsonProcessingException {
        LatticeCacheSeedChkConfig conf = new LatticeCacheSeedChkConfig();
        conf.setDomain("Domain");
        conf.setDuns("DUNS");
        conf.setExceededCountThreshold(10);
        conf.setLessThanThresholdFlag(false);
        List<String> idFields = new ArrayList<String>();
        idFields.add("Domain");
        idFields.add("DUNS");
        conf.setId(idFields);
        List<String> fieldList = new ArrayList<String>();
        fieldList.add("Domain");
        fieldList.add("DUNS");
        conf.setGroupByFields(fieldList);
        conf.setCheckField("DUNS");
        conf.setThreshold(100.00);
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
            { "EmptyField", "null,DUNS16", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "EmptyField", "null,null", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "EmptyField", "null,DUNS16", null, "Domain", null, "Field [Domain] should not be null or empty." }, //
            { "BelowExpectedCount", null, null, "__COUNT__", "8", "Total count of records below [10]" }, //
            { "UnderPopulatedField", null, null, "DUNS", "87.50",
                    "Population of field [DUNS] is [87.50] percent, lower than [12.50] percent" }, //
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
        Assert.assertEquals(rowCount, 8);
    }

}
