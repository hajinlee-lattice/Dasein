package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ManualSeedCleanFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedCleanTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ManualSeedCleanTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("ManualSeedCleanedData");

    GeneralSource baseSource = new GeneralSource("ManualSeedData");

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
            step1.setTransformer(ManualSeedCleanFlow.TRANSFORMER_NAME);
            String confParamStr1 = getManualSeedConfig();
            step1.setConfiguration(confParamStr1);
            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTransformer("standardizationTransformer");
            step2.setTargetSource(source.getSourceName());
            String confParamStr2 = getRangeMappingConfig();
            step2.setConfiguration(confParamStr2);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getManualSeedConfig() throws JsonProcessingException {
        ManualSeedCleanTransformerConfig conf = new ManualSeedCleanTransformerConfig();
        conf.setSalesVolumeInUSDollars("Manual_SALES_VOLUME_US_DOLLARS");
        conf.setEmployeesTotal("Manual_EMPLOYEES_TOTAL");
        conf.setManSeedDomain("Manual_Domain");
        conf.setManSeedDuns("Manual_Duns");
        return JsonUtils.serialize(conf);
    }

    private String getRangeMappingConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] addConsolidatedRangeFields = { "Manual_LE_EMPLOYEE_RANGE", "Manual_LE_REVENUE_RANGE" };
        conf.setAddConsolidatedRangeFields(addConsolidatedRangeFields);
        ConsolidateRangeStrategy[] strategies = { ConsolidateRangeStrategy.MAP_VALUE,
                ConsolidateRangeStrategy.MAP_VALUE };
        conf.setConsolidateRangeStrategies(strategies);
        String[] rangeInputFields = { "Manual_EMPLOYEES_TOTAL", "Manual_SALES_VOLUME_US_DOLLARS" };
        conf.setRangeInputFields(rangeInputFields);
        String[] rangeMapFileNames = { "EmployeeRangeMapping.txt", "ManualSeedRevenueRangeMap.txt" };
        conf.setRangeMapFileNames(rangeMapFileNames);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = {
                StandardizationStrategy.CONSOLIDATE_RANGE };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private void prepareManualSeedData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Manual_Id", String.class));
        columns.add(Pair.of("Manual_SALES_VOLUME_US_DOLLARS", String.class));
        columns.add(Pair.of("Manual_EMPLOYEES_TOTAL", String.class));
        columns.add(Pair.of("Manual_Domain", String.class));
        columns.add(Pair.of("Manual_Duns", String.class));
        Object[][] data = new Object[][] {
                { "1", "$18.16", "5,001-10,000", "netflix.com", "123123" },
                { "2", "$3.93", "5203", "netapp.com", "456456" }, { "3", "$21.04", ">10,000Œæ", "datos.com", "131313" },
                { "4", "$35.97", "10001", "apple.com", "413131" },
                { "5", "$5.42", "14,024-20,000", "ms.com", "289922" },
                { "6", "$9.92", "15098", "netapp.com", "456456" }, { "7", "$9.86", "23172", "netflix.com", "123123" },
                { "8", "$31.39", "", "payless.com", "898989" }, { "9", "$10.86", "80,000", "target.com", "689383" },
                { "10", "$22.76", "18,200", "macys.com", "783921" }, { "11", "", "", "nordstorm.com", "891824" },
                { "12", null, null, "netflix.com", "123123" },
                { "13", "$0.010999999", "6203", "craigslist.com", "324211" },
                { "14", "$0.050999999", "6000", "openstack.com", "324212" },
                { "15", "$0.100999999", "7000", "karlx.com", "424254" },
                { "16", "$0.250999998", "8000", "pearlx.com", "242455" },
                { "17", "$0.500999999", "9000", "oracle.com", "435256" } };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    private Object[][] expectedDataValues = { { "1", 18160000000L, null, null, ">10B", "netflix.com", "123123" },
            { "3", 21040000000L, null, null, ">10B", "datos.com", "131313" },
            { "4", 35970000000L, 10001, ">10,000", ">10B", "apple.com", "413131" },
            { "5", 5420000000L, null, null, "5B-10B", "ms.com", "289922" },
            { "6", 9920000000L, 15098, ">10,000", "5B-10B", "netapp.com", "456456" },
            { "8", 31390000000L, null, null, ">10B", "payless.com", "898989" },
            { "9", 10860000000L, 80000, ">10,000", ">10B", "target.com", "689383" },
            { "10", 22760000000L, 18200, ">10,000", ">10B", "macys.com", "783921" },
            { "11", null, null, null, null, "nordstorm.com", "891824" },
            { "13", 10999999L, 6203, "5001-10,000", "1-10M", "craigslist.com", "324211" },
            { "14", 50999999L, 6000, "5001-10,000", "11-50M", "openstack.com", "324212" },
            { "15", 100999999L, 7000, "5001-10,000", "51-100M", "karlx.com", "424254" },
            { "16", 250999998L, 8000, "5001-10,000", "101-250M", "pearlx.com", "242455" },
            { "17", 500999999L, 9000, "5001-10,000", "251-500M", "oracle.com", "435256" } };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[0]), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("Manual_Id"));
            Object[] expected = expectedData.get(id);
            Assert.assertTrue(isObjEquals(record.get("Manual_SALES_VOLUME_US_DOLLARS"), expected[1]));
            Assert.assertTrue(isObjEquals(record.get("Manual_EMPLOYEES_TOTAL"), expected[2]));
            Assert.assertTrue(isObjEquals(record.get("Manual_LE_EMPLOYEE_RANGE"), expected[3]));
            Assert.assertTrue(isObjEquals(record.get("Manual_LE_REVENUE_RANGE"), expected[4]));
            Assert.assertTrue(isObjEquals(record.get("Manual_Domain"), expected[5]));
            Assert.assertTrue(isObjEquals(record.get("Manual_Duns"), expected[6]));
            rowCount++;
        }
        Assert.assertEquals(rowCount, 14);
    }
}
