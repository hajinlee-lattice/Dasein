package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.SourceVersionDiff;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMValidatorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ExceedVersionDiffCheckTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("CompreSrcCurrVersWithPrevVers");
    GeneralSource baseSource1 = new GeneralSource("AccountMaster");
    private static final String VERSION1 = "2017-11-14_01-53-51_UTC";
    private static final String VERSION2 = "2017-11-16_05-41-36_UTC";

    private static final Logger log = LoggerFactory.getLogger(ExceedVersionDiffCheckTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
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
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AccountMasterCompare");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(SourceVersionDiff.TRANSFORMER_NAME);
            String confParamStr1 = getAmValidateConfig();
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

    private String getAmValidateConfig() throws JsonProcessingException {
        AMValidatorConfig conf = new AMValidatorConfig();
        conf.setDomain("LDC_Domain");
        conf.setDuns("LDC_DUNS");
        conf.setLatticeId("LatticeID");
        conf.setNotNullField("LDC_Domain");
        conf.setNullField("LDC_DUNS");
        conf.setDiffVersion(VERSION2);
        conf.setDiffVersionCompared(VERSION1);
        conf.setThreshold(5.0);
        return JsonUtils.serialize(conf);
    }

    Object[][] data = new Object[][] { { null, "sbi.com", "DUNS12", "Mumbai" },
            { 100001L, "adobe.com", "DUNS22", "San Jose" }, { 100002L, "adobe.com", "DUNS22", "San Jose" },
            { 100003L, "yahoo.com", "DUNS34", "Milpitas" }, { null, "data.com", null, "San Mateo" },
            { 100005L, "yp.com", "DUNS12", "Los Angeles" }, { 100006L, "adobe.com", "DUNS22", "Foster City" },
            { 100001L, "matrix.com", "DUNS33", "Burlingame" }, { 100008L, "yahoo.com", "DUNS34", "Sunnyvale" },
            { null, null, "DUNS87", "Palo Alto" }, { null, null, null, null }, { null, null, "DUNS87", "Palo Alto" },
            { 100010L, "yahoo.com", "DUNS34", "Sunnyvale" }, { 100055L, "yp.com", "DUNS12", "Los Angeles" },
            { 100011L, "adobe.com", "", "San Jose" }, { 100052L, "adobe.com", null, "San Jose" },
            { 100018L, "yahoo.com", null, "Sunnyvale" }, { 100058L, "yahoo.com", "", "Sunnyvale" },
            { 100028L, "yahoo.com", "", "Sunnyvale" }, { 100048L, "yahoo.com", "", "Sunnyvale" },
            { 100038L, "yahoo.com", null, "Sunnyvale" } };

    Object[][] dataCompared = new Object[][] { { null, "netapp.com", "", "Mumbai" },
            { 100001L, "microsoft.com", "DUNS88", "San Jose" }, { 100002L, "paypal.com", "DUNS98", "San Jose" },
            { 100003L, "dell.com", "DUNS74", "Milpitas" }, { null, "comcast.com", null, "San Mateo" },
            { 100005L, "craigslist.com", null, "Los Angeles" }, { 100006L, "qualcom.com", null, "Foster City" },
            { 100001L, "intel.com", "DUNS39", "Burlingame" }, { 100008L, "lnt.com", "DUNS94", "Sunnyvale" },
            { null, null, "DUNS87", "Palo Alto" }, { null, null, null, null } };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("LatticeID", Long.class));
        schema.add(Pair.of("LDC_Domain", String.class));
        schema.add(Pair.of("LDC_DUNS", String.class));
        schema.add(Pair.of("LDC_City", String.class));

        uploadBaseSourceData(baseSource1.getSourceName(), VERSION1, schema, dataCompared);
        uploadBaseSourceData(baseSource1.getSourceName(), VERSION2, schema, data);

        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(baseSource1, VERSION1).toString();
            List<String> src2Files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema src2Schema = AvroUtils.getSchema(yarnConfiguration, new Path(src2Files.get(0)));

            String avscPath1 = hdfsPathBuilder.constructSchemaFile(baseSource1.getSourceName(), VERSION1).toString();
            String avscPath2 = hdfsPathBuilder.constructSchemaFile(baseSource1.getSourceName(), VERSION2).toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath1, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath2, src2Schema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for AccountMaster", e);
        }
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[][] expectedData = new Object[][] { //
            { "ExceededVersionDiffForNumOfBusinesses", null, "__COUNT__", "62.50",
            "Number of Businesses changes [62.50] percent (from [11] to [21]), more than [57.50] percent"}, //
        {"ExceededVersionDiffForDomOnly", null, "__COUNT__", "66.67", "Number of Domain Only Records changes "
                + "[66.67] percent (from [4] to [8])"}, //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object chkCode = record.get(DataCloudConstants.CHK_ATTR_CHK_CODE);
            Object checkField = record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD);
            Object checkValue = record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE);
            Object chkMessage = record.get(DataCloudConstants.CHK_ATTR_CHK_MSG);
            if (isObjEquals(chkCode, expectedData[0][0])) {
                Assert.assertTrue(isObjEquals(checkField, expectedData[0][2]));
                Assert.assertTrue(isObjEquals(checkValue, expectedData[0][3]));
                Assert.assertTrue(isObjEquals(chkMessage, expectedData[0][4]));
            }
            if (isObjEquals(chkCode, expectedData[1][0])) {
                Assert.assertTrue(isObjEquals(checkField, expectedData[1][2]));
                Assert.assertTrue(isObjEquals(checkValue, expectedData[1][3]));
                Assert.assertTrue(isObjEquals(chkMessage, expectedData[1][4]));
            }
            rowCount++;
            log.info("record : " + record);
        }
        Assert.assertEquals(rowCount, 2);
    }
}
