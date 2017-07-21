package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ManualSeedCleanFlow;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ManualSeedCleanTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ManualSeedCleanTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("ManualSeedCleanedData");

    GeneralSource baseSource = new GeneralSource("ManualSeedData");

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    @Autowired
    SourceService sourceService;

    String targetSourceName = "ManualSeedCleanedData";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareManualSeedData();
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
            configuration.setName("ManualSeedClean");
            configuration.setVersion(targetVersion);
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(targetSourceName);
            step1.setTransformer(ManualSeedCleanFlow.TRANSFORMER_NAME);
            String confParamStr1 = getManualSeedConfig();
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

    private String getManualSeedConfig() throws JsonProcessingException {
        ManualSeedCleanTransformerConfig conf = new ManualSeedCleanTransformerConfig();
        conf.setSalesVolumeInUSDollars("SALES_VOLUME_US_DOLLARS");
        conf.setEmployeesTotal("EMPLOYEES_TOTAL");
        return om.writeValueAsString(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    private void prepareManualSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Id", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", String.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL", String.class));
        Object[][] data = new Object[][] {
                { "1", "$18.16", "5,001-10,000" },
                { "2", "$3.93", "5203", }, { "3", "$21.04", ">10,000Œæ" },
                { "4", "$35.97", "10001" }, { "5", "$5.42", "14,024-20,000" }, { "6", "$9.92", "15098" },
                { "7", "$9.86", "23172" }, { "8", "$31.39", "" }, { "9", "$10.86", "80,000" },
                { "10", "$22.76", "18,200" } };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Object[][] expectedData = new Object[][] {
                { "1", "18160000000", null }, { "2", "3930000000", "5203" },
                { "3", "21040000000", null }, { "4", "35970000000", "10001" }, { "5", "5420000000", null },
                { "6", "9920000000", "15098" }, { "7", "9860000000", "23172" }, { "8", "31390000000", null },
                { "9", "10860000000", "80000" },
                { "10", "22760000000", "18200" } };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String employeesTotal = null;
            if(record.get("EMPLOYEES_TOTAL") != null)
                employeesTotal = record.get("EMPLOYEES_TOTAL").toString();
            String salesVolumeInDollars = record.get("SALES_VOLUME_US_DOLLARS").toString();
            String id = record.get("Id").toString();
            int counter = Integer.parseInt(id);
            Assert.assertEquals(id, expectedData[counter - 1][0]);
            Assert.assertEquals(salesVolumeInDollars, expectedData[counter - 1][1]);
            Assert.assertEquals(employeesTotal, expectedData[counter - 1][2]);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 10);
    }

}
