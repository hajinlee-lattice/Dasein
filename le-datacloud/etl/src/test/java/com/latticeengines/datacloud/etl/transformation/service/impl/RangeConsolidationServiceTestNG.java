package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class RangeConsolidationServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(RangeConsolidationServiceTestNG.class);

    GeneralSource source = new GeneralSource("ConsolidateRange");

    GeneralSource baseSource = new GeneralSource("ConsolidateRange_Test");

    String targetSourceName = "ConsolidateRange";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "ConsolidateRange_Test", baseSourceVersion);
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
            configuration.setName("RangeConsolidation");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("ConsolidateRange_Test");
            step1.setBaseSources(baseSources);
            step1.setTransformer("standardizationTransformer");
            step1.setTargetSource("ConsolidateRange");
            String confParamStr1 = getRangeMappingConfig();
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

    private String getRangeMappingConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] addConsolidatedRangeFields = { "ConsolidateRange", "ConsolidateValue", "ConsolidateLargeValue" };
        conf.setAddConsolidatedRangeFields(addConsolidatedRangeFields);
        ConsolidateRangeStrategy[] strategies = { ConsolidateRangeStrategy.MAP_RANGE,
                ConsolidateRangeStrategy.MAP_VALUE, ConsolidateRangeStrategy.MAP_VALUE };
        conf.setConsolidateRangeStrategies(strategies);
        String[] rangeInputFields = { "Range", "Value", "LargeValue" };
        conf.setRangeInputFields(rangeInputFields);
        String[] rangeMapFileNames = { "OrbRevenueRangeMapping.txt", "EmployeeRangeMapping.txt",
                "EmployeeRangeMapping.txt" };
        conf.setRangeMapFileNames(rangeMapFileNames);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = {
                StandardizationStrategy.CONSOLIDATE_RANGE };
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
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[][] expectedData = { { "0-1M", "201-500", ">10,000" }, { "null", "null", "null" },
                { "101-250M", "0", "1-10" } };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String expectedRange1 = String.valueOf(record.get("ConsolidateRange"));
            String expectedRange2 = String.valueOf(record.get("ConsolidateValue"));
            String expectedRange3 = String.valueOf(record.get("ConsolidateLargeValue"));
            boolean flag = false;
            for (String[] data : expectedData) {
                if (expectedRange1.equals(data[0]) && expectedRange2.equals(data[1])
                        && expectedRange3.equals(data[2])) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
    }
}
