package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

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

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.seed.DnBCleanFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DnBCleanTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DnBCleanTestNG.class);

    private GeneralSource source = new GeneralSource("DnBCacheSeedClean");
    private GeneralSource baseSource = new GeneralSource("DnBCacheSeed");

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareDnBCacheSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DnBClean");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(baseSource.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(DnBCleanFlow.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        step1.setConfiguration("{}");

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private Object[][] expectedData = new Object[][] {
            { 1, null, null, 1, null, 1, null }, //
            { 2, null, "2", 1, null, 1, null }, //
            { 3, 1L, null, null, null, 1, null }, //
            { 4, 1L, null, null, "2", 1, null }, //
            { 5, 1L, null, 1, null, null, null }, //
            { 6, 1L, null, 1, null, null, "2" }, //
            { 7, null, null, null, null, null, null }, //
            { 8, 0L, "1", 0, "1", 0, "1" }, //
            { 9, null, null, null, null, null, null }, //
    };

    // ID, SALES_VOLUME_US_DOLLARS, SALES_VOLUME_RELIABILITY_CODE,
    // EMPLOYEES_TOTAL, EMPLOYEES_TOTAL_RELIABILITY_CODE,
    // EMPLOYEES_HERE, EMPLOYEES_HERE_RELIABILITY_CODE
    private Object[][] data = new Object[][] { //
            { 1, 0L, null, 1, null, 1, null }, //
            { 2, 0L, "2", 1, null, 1, null }, //
            { 3, 1L, null, 0, null, 1, null }, //
            { 4, 1L, null, 0, "2", 1, null }, //
            { 5, 1L, null, 1, null, 0, null }, //
            { 6, 1L, null, 1, null, 0, "2" }, //
            { 7, 0L, null, 0, null, 0, null }, //
            { 8, 0L, "1", 0, "1", 0, "1" }, //
            { 9, null, null, null, null, null, null }, //
    };

    private void prepareDnBCacheSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("SALES_VOLUME_RELIABILITY_CODE", String.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL", Integer.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL_RELIABILITY_CODE", String.class));
        columns.add(Pair.of("EMPLOYEES_HERE", Integer.class));
        columns.add(Pair.of("EMPLOYEES_HERE_RELIABILITY_CODE", String.class));
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<Integer, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedMap.put((Integer) data[0], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedResult = expectedMap.get(record.get("ID"));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_US_DOLLARS"), expectedResult[1]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_RELIABILITY_CODE"), expectedResult[2]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_TOTAL"), expectedResult[3]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_TOTAL_RELIABILITY_CODE"), expectedResult[4]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_HERE"), expectedResult[5]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_HERE_RELIABILITY_CODE"), expectedResult[6]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedMap.size());
    }
}
