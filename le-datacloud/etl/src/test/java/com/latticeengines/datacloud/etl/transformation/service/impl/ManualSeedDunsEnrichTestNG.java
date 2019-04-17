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
import com.latticeengines.datacloud.dataflow.transformation.ManualSeedEnrichDunsFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedEnrichDunsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ManualSeedDunsEnrichTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ManualSeedDunsEnrichTestNG.class);

    GeneralSource source = new GeneralSource("ManualSeedEnrichedDuns");

    GeneralSource baseSource1 = new GeneralSource("ManualSeedData");
    GeneralSource baseSource2 = new GeneralSource("ManualSeedWithoutZip");
    GeneralSource baseSource3 = new GeneralSource("ManualSeedWithoutZipAndCity");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareManualSeedDuns();
        prepareManSeedNoZipDuns();
        prepareManSeedNoZipCityDuns();
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
            configuration.setName("ManualSeedEnrichDuns");
            configuration.setVersion(targetVersion);

            // Initialize manualSeed Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource1.getSourceName());
            baseSourceStep1.add(baseSource2.getSourceName());
            baseSourceStep1.add(baseSource3.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(ManualSeedEnrichDunsFlow.TRANSFORMER_NAME);
            String confParamStr = getManualSeedDataConfig();
            step1.setConfiguration(confParamStr);

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

    private void prepareManualSeedDuns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("MAN_Id", String.class));
        columns.add(Pair.of("MAN_Domain", String.class));
        columns.add(Pair.of("MAN_DUNS", String.class));
        Object[][] data = new Object[][] {
                { "1", "sbi.com", "DUNS12" }, { "2", "adobe.com", "DUNS22" }, { "3", "netapp.com", "DUNS14" },
                { "4", "cisco.com", null }, { "5", "data.com", null }, { "6", "yp.com", null },
                { "7", "conviva.com", null }, { "8", "matrix.com", null }, { "9", "yahoo.com", null },
                { "10", "aptus.com", null } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareManSeedNoZipDuns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("MAN_Id", String.class));
        columns.add(Pair.of("MAN_Domain", String.class));
        columns.add(Pair.of("MAN_DUNS", String.class));
        Object[][] data = new Object[][] {
                { "1", "sbi.com", "DUNS33" }, { "2", "adobe.com", "DUNS22" }, { "3", "netapp.com", "DUNS44" },
                { "4", "cisco.com", "DUNS67" }, { "5", "data.com", "DUNS21" }, { "6", "yp.com", null },
                { "7", "conviva.com", "DUNS89" }, { "8", "matrix.com", null }, { "9", "yahoo.com", "DUNS50" },
                { "10", "aptus.com", "DUNS11" }
        };
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareManSeedNoZipCityDuns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("MAN_Id", String.class));
        columns.add(Pair.of("MAN_Domain", String.class));
        columns.add(Pair.of("MAN_DUNS", String.class));
        Object[][] data = new Object[][] {
                { "1", "sbi.com", "DUNS33" }, { "2", "adobe.com", "DUNS22" }, { "3", "netapp.com", "DUNS45" },
                { "4", "cisco.com", "DUNS67" }, { "5", "data.com", "DUNS21" }, { "6", "yp.com", "DUNS77" },
                { "7", "conviva.com", "DUNS89" }, { "8", "matrix.com", null }, { "9", "yahoo.com", "DUNS99" },
                { "10", "aptus.com", "DUNS12" }
        };
        uploadBaseSourceData(baseSource3.getSourceName(), baseSourceVersion, columns, data);
    }

    private String getManualSeedDataConfig() throws JsonProcessingException {
        ManualSeedEnrichDunsConfig conf = new ManualSeedEnrichDunsConfig();
        conf.setManSeedDuns("MAN_DUNS");
        conf.setManSeedId("MAN_Id");
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] expectedDataValues = {
            { "7", "conviva.com", "DUNS89" }, { "6", "yp.com", "DUNS77" }, { "4", "cisco.com", "DUNS67" },
            { "8", "matrix.com", null }, { "1", "sbi.com", "DUNS12" }, { "2", "adobe.com", "DUNS22" },
            { "3", "netapp.com", "DUNS14" }, { "5", "data.com", "DUNS21" }, { "9", "yahoo.com", "DUNS50" },
            { "10", "aptus.com", "DUNS11" } };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[0]), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("MAN_Id"));
            Object[] expected = expectedData.get(id);
            Assert.assertTrue(isObjEquals(record.get("MAN_Domain"), expected[1]));
            Assert.assertTrue(isObjEquals(record.get("MAN_DUNS"), expected[2]));
            rowCount++;
        }
        Assert.assertEquals(rowCount, 10);
    }
}
