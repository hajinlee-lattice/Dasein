package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.AccMastrManChkReportGenFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMastrManChkReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccMastrManChkReportGenerateTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    GeneralSource source = new GeneralSource("AccMastrManChkReportAnalysis");
    GeneralSource baseSource1 = new GeneralSource("LatticeCacheSeedChk");
    GeneralSource baseSource2 = new GeneralSource("DnBCacheSeedChk");
    GeneralSource baseSource3 = new GeneralSource("AccountMasterSeedChk");
    GeneralSource baseSource4 = new GeneralSource("AccountMasterIdChk");
    GeneralSource baseSource5 = new GeneralSource("AccountMasterChk");
    GeneralSource baseSource6 = new GeneralSource("AccountMasterLookupChk");
    GeneralSource baseSource7 = new GeneralSource("ManualSeedChk");

    public static final String COUNT = "Count";

    private static final Logger log = LoggerFactory.getLogger(AccMastrManChkReportGenerateTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private Object[][] data1 = new Object[][] {
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

    private Object[][] data2 = new Object[][] {
            { "EmptyField", "yahoo.com,", null, "DUNS", "", "Field [DUNS] should not be null or empty." }, //
            { "EmptyField", "null,null", null, "DUNS", null, "Field [DUNS] should not be null or empty." }, //
            { "BelowExpectedCount", null, null, "__COUNT__", "8", "Total count of records below [10]" }, //
    };

    private Object[][] data3 = new Object[][] {
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

    private Object[][] data4 = new Object[][] {
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

    private Object[][] data5 = new Object[][] {
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
                    "Found duplicated value [1007] in the field [[LatticeID]]." } //
    };

    private Object[][] data6 = new Object[][] {
        { "DuplicatedValue", null, "key1", "[Key]", "2", "Found duplicated value [key1] in the field [[Key]]." }, //
        { "DuplicatedValue", null, "key2", "[Key]", "3", "Found duplicated value [key2] in the field [[Key]]." }, //
        { "BelowExpectedCount", null, null, "__COUNT__", "8", "Total count of records below [10]" }, //
    };

    private Object[][] data7 = new Object[][] {
            { "DuplicatedValue", null, "netapp.com,DUNS11", "[Domain, DUNS]", "2",
                    "Found duplicated value [netapp.com,DUNS11] in the field [[Domain, DUNS]]." }, //
            { "DuplicatedValue", null, "oracle.com,DUNS14", "[Domain, DUNS]", "2",
                    "Found duplicated value [oracle.com,DUNS14] in the field [[Domain, DUNS]]." }, //
            { "DuplicatedValue", null, "null,DUNS16", "[Domain, DUNS]", "2",
                    "Found duplicated value [null,DUNS16] in the field [[Domain, DUNS]]." }, //
    };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("CheckCode", String.class));
        schema.add(Pair.of("RowId", String.class));
        schema.add(Pair.of("GroupId", String.class));
        schema.add(Pair.of("CheckField", String.class));
        schema.add(Pair.of("CheckValue", String.class));
        schema.add(Pair.of("CheckMessage", String.class));
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data1);
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, schema, data2);

        uploadBaseSourceData(baseSource3.getSourceName(), baseSourceVersion, schema, data3);
        uploadBaseSourceData(baseSource4.getSourceName(), baseSourceVersion, schema, data4);

        uploadBaseSourceData(baseSource5.getSourceName(), baseSourceVersion, schema, data5);
        uploadBaseSourceData(baseSource6.getSourceName(), baseSourceVersion, schema, data6);
        uploadBaseSourceData(baseSource7.getSourceName(), baseSourceVersion, schema, data7);

        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(baseSource1, baseSourceVersion)
                    .toString();
            List<String> src2Files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema src2Schema = AvroUtils.getSchema(yarnConfiguration, new Path(src2Files.get(0)));
            src2Schema.addProp(COUNT, "10");
            String avscPath1 = hdfsPathBuilder.constructSchemaFile(baseSource1.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath2 = hdfsPathBuilder.constructSchemaFile(baseSource2.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath3 = hdfsPathBuilder.constructSchemaFile(baseSource3.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath4 = hdfsPathBuilder.constructSchemaFile(baseSource4.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath5 = hdfsPathBuilder.constructSchemaFile(baseSource5.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath6 = hdfsPathBuilder.constructSchemaFile(baseSource6.getSourceName(), baseSourceVersion)
                    .toString();
            String avscPath7 = hdfsPathBuilder.constructSchemaFile(baseSource7.getSourceName(), baseSourceVersion)
                    .toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath1, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath2, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath3, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath4, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath5, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath6, src2Schema.toString());
            HdfsUtils.writeToFile(yarnConfiguration, avscPath7, src2Schema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for required sources", e);
        }
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
            configuration.setName("AccMastrManChkReportAnalysis");
            configuration.setVersion(targetVersion);

            // Initialize manualSeed Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource1.getSourceName());
            baseSources.add(baseSource2.getSourceName());
            baseSources.add(baseSource3.getSourceName());
            baseSources.add(baseSource4.getSourceName());
            baseSources.add(baseSource5.getSourceName());
            baseSources.add(baseSource6.getSourceName());
            baseSources.add(baseSource7.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(AccMastrManChkReportGenFlow.TRANSFORMER_NAME);
            String confParamStr = getSourcesReportConfig();
            step1.setConfiguration(confParamStr);
            step1.setTargetSource(source.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSourcesReportConfig() throws JsonProcessingException {
        AccMastrManChkReportConfig conf = new AccMastrManChkReportConfig();
        conf.setCheckCode("CheckCode");
        conf.setRowId("RowId");
        conf.setGroupId("GroupId");
        conf.setCheckValue("CheckValue");
        conf.setCheckMessage("CheckMessage");
        conf.setCheckField("CheckField");
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[] expectedDataValues = new Object[] { //
            "2 Rows/groups violates DuplicatedValue check on fields [Key]", //
            "2 Rows/groups violates EmptyField check on fields DUNS", //
            "3 Rows/groups violates OutOfCoverageValForRow check on fields DomainSource", //
            "Total count of records below [10]", //
            "Total count of records below [10]", //
            "Total count of records below [10]", //
            "Total count of records below [10]", //
            "Total count of records below [10]", //
            "Population of field [DUNS] is [87.50] percent, lower than [12.50] percent", //
            "2 Rows/groups violates DuplicatedValue check on fields [LatticeID]", //
            "No record found for field [DomainSource] with group value [Orb,Manual].", //
            "1 Rows/groups violates DuplicatedValue check on fields [RedirectFromId]", //
            "3 Rows/groups violates DuplicatedValue check on fields [Domain, LDC_DUNS]", //
            "6 Rows/groups violates EmptyField check on fields Domain", //
            "11 Rows/groups violates DuplicatedValue check on fields [Domain, DUNS]", //
            "Field [LatticeID] should not be null or empty.", //
            "1 Rows/groups violates DuplicatedValue check on fields [LatticeAccountId]", //
            "1 Rows/groups violates DuplicatedValuesWithStatus check on fields [LatticeID]" };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        HashMap<String, Integer> storeMsg = new HashMap<String, Integer>();
        for (Object msgVal : expectedDataValues) {
            storeMsg.put(String.valueOf(msgVal), storeMsg.getOrDefault(msgVal, 0) + 1);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String message = String.valueOf(record.get(DataCloudConstants.CHK_ATTR_CHK_MSG));
            if (storeMsg.containsKey(message) && storeMsg.get(message) >= 1)
                storeMsg.put(message, storeMsg.get(message) - 1);
            else
                Assert.fail("Unexpected record found");
            rowCount++;
            log.info("record : " + record);
        }
        Assert.assertEquals(rowCount, 18);
    }

}
