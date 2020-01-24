package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.SourceStandardizationFlow;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.IDStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class BasicStandardizationServiceTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(BasicStandardizationServiceTestNG.class);

    private GeneralSource source = new GeneralSource("Output");
    private GeneralSource baseSource = new GeneralSource("Input");
    private GeneralSource intermediateSource = new GeneralSource("Intermediate");

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        prepareInput();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmSchema();
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("BasicStandardization");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(baseSource.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(SourceStandardizationFlow.TRANSFORMER_NAME);
        step1.setTargetSource(intermediateSource.getSourceName());
        String confParamStr1 = getFirstStepConfig();
        step1.setConfiguration(confParamStr1);

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);

        step2.setTransformer(SourceStandardizationFlow.TRANSFORMER_NAME);
        step2.setTargetSource(source.getSourceName());
        String confParamStr2 = getSecondStepConfig();
        step2.setConfiguration(confParamStr2);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getFirstStepConfig() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] updateFields = { "CHIEF_EXECUTIVE_OFFICER_NAME" };
        String[] updateExpressions = { "ID == 2 ? \"CEO2Fixed\" : CHIEF_EXECUTIVE_OFFICER_NAME" };
        String[][] updateInputFields = { { "ID", "CHIEF_EXECUTIVE_OFFICER_NAME" } };
        conf.setUpdateFields(updateFields);
        conf.setUpdateExpressions(updateExpressions);
        conf.setUpdateInputFields(updateInputFields);
        conf.setShouldInheritSchemaProp(true);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.UPDATE };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getSecondStepConfig() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] countryFields = { "Country" };
        conf.setCountryFields(countryFields);
        String[] stateFields = { "State" };
        conf.setStateFields(stateFields);
        String[] zipcodeFields = { "ZipCode" };
        conf.setStringFields(zipcodeFields);
        String[] dunsFields = { "DUNS" };
        conf.setDunsFields(dunsFields);
        String[] idFields = {"RowId", "UUID"};
        conf.setIdFields(idFields);
        IDStrategy[] idStrategies = { IDStrategy.ROWID, IDStrategy.UUID };
        conf.setIdStrategies(idStrategies);
        String[][] copyFields = { { "Name", "CopiedName" } };
        conf.setCopyFields(copyFields);
        conf.setChecksumField("Checksum");
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { //
                StandardizationStrategy.COUNTRY, StandardizationStrategy.STATE, //
                StandardizationStrategy.STRING, StandardizationStrategy.DUNS, //
                StandardizationStrategy.ADD_ID, StandardizationStrategy.COPY, //
                StandardizationStrategy.CHECKSUM };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    // ID, Name, Country, State, ZipCode, DUNS, CHIEF_EXECUTIVE_OFFICER_NAME
    private Object[][] input = new Object[][] { //
            { 1, "Name1", "United States", "CA", "  94404  ", "0123456789", null }, //
            { 2, "Name2", "England", "Scotland &.", " null ", "123456789", "CEO2" }, //
            { 3, "Name3", null, "Scotland &.", "", "6789", "CEO3" }, //
            { 4, "Name4", "USA", null, "none", null, "" }, //
    };

    private void prepareInput() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("ZipCode", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("CHIEF_EXECUTIVE_OFFICER_NAME", String.class));
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, input);

        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion).toString();
            List<String> srcFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema srcSchema = AvroUtils.getSchema(yarnConfiguration, new Path(srcFiles.get(0)));
            srcSchema.addProp("PropertyToRetain", "TestPropertyToRetain");

            String avscPath = hdfsPathBuilder.constructSchemaFile(baseSource.getSourceName(), baseSourceVersion)
                    .toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, srcSchema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for AccountMaster", e);
        }
    }

    private Object[][] expected = { //
            { 1, "Name1", "USA", "CALIFORNIA", "94404", null, "Name1", null }, //
            { 2, "Name2", "UNITED KINGDOM", "SCOTLAND", null, "123456789", "Name2", "CEO2Fixed" }, //
            { 3, "Name3", null, null, null, "000006789", "Name3", "CEO3" }, //
            { 4, "Name4", "USA", null, null, null, "Name4", "" } //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<Integer, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expected) {
            expectedMap.put((Integer) data[0], data);
        }
        int rowNum = 0;
        Set<Long> rowIdSet = new HashSet<>();
        Set<String> uuidSet = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedResult = expectedMap.get(record.get("ID"));
            Assert.assertTrue(isObjEquals(record.get("Name"), expectedResult[1]));
            Assert.assertTrue(isObjEquals(record.get("Country"), expectedResult[2]));
            Assert.assertTrue(isObjEquals(record.get("State"), expectedResult[3]));
            Assert.assertTrue(isObjEquals(record.get("ZipCode"), expectedResult[4]));
            Assert.assertTrue(isObjEquals(record.get("DUNS"), expectedResult[5]));
            Assert.assertTrue(isObjEquals(record.get("CopiedName"), expectedResult[6]));
            Assert.assertTrue(isObjEquals(record.get("CHIEF_EXECUTIVE_OFFICER_NAME"), expectedResult[7]));
            Assert.assertFalse(rowIdSet.contains(record.get("RowId")));
            rowIdSet.add((Long) record.get("RowId"));
            Assert.assertFalse(uuidSet.contains(record.get("UUID").toString()));
            uuidSet.add(record.get("UUID").toString());
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }

    private void confirmSchema() {
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(intermediateSource, targetVersion);
        Assert.assertEquals(schema.getProp("PropertyToRetain"), "TestPropertyToRetain");
    }
}
