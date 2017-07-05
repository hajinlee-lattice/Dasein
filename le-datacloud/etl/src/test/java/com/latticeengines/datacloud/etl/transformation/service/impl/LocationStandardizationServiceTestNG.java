package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.SourceStandardizationFlow;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class LocationStandardizationServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(LocationStandardizationServiceTestNG.class);

    GeneralSource source = new GeneralSource("LocationStandard");

    GeneralSource baseSource = new GeneralSource("LocationInput");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareLocations();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("LocationStandardization");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SourceStandardizationFlow.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getLocationStandardizationConfig();
            step1.setConfiguration(confParamStr1);

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

    private String getLocationStandardizationConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] countryFields = { "Country" };
        conf.setCountryFields(countryFields);
        String[] stateFields = { "State" };
        conf.setStateFields(stateFields);
        String[] zipcodeFields = { "ZipCode" };
        conf.setStringFields(zipcodeFields);
        String[] dunsFields = { "DUNS" };
        conf.setDunsFields(dunsFields);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.COUNTRY,
                StandardizationStrategy.STATE, StandardizationStrategy.STRING, StandardizationStrategy.DUNS };
        conf.setSequence(sequence);
        return om.writeValueAsString(conf);
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
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] locations = new Object[][] { //
            { 1, "Name1", "United States", "CA", "  94404  ", "0123456789" }, //
            { 2, "Name2", "England", "Scotland &.", null, "123456789" }, //
            { 3, "Name3", null, "Scotland &.", "", "6789" }, //
            { 4, "Name4", "USA", null, null, null }, //
    };

    private void prepareLocations() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("ZipCode", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, locations);
    }

    private Object[][] expected = { //
            { 1, "Name1", "USA", "CALIFORNIA", "94404", null }, //
            { 2, "Name2", "UNITED KINGDOM", "SCOTLAND", null, "123456789" }, //
            { 3, "Name3", null, null, null, "000006789" }, //
            { 4, "Name4", "USA", null, null, null } //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<Integer, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expected) {
            expectedMap.put((Integer) data[0], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record);
            Object[] expectedResult = expectedMap.get((Integer) record.get("ID"));
            Assert.assertTrue(equals(record.get("Name"), expectedResult[1]));
            Assert.assertTrue(equals(record.get("Country"), expectedResult[2]));
            Assert.assertTrue(equals(record.get("State"), expectedResult[3]));
            Assert.assertTrue(equals(record.get("ZipCode"), expectedResult[4]));
            Assert.assertTrue(equals(record.get("DUNS"), expectedResult[5]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }

}
