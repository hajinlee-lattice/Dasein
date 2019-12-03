package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.seed.SeedDomainDunsCleanup;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.SeedDomainDunsCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;


public class SeedDomainDunsCleanupTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(SeedDomainDunsCleanupTestNG.class);

    private GeneralSource source = new GeneralSource("SeedCleaned");
    private GeneralSource seed = new GeneralSource("Seed");
    private GeneralSource goldenDom = new GeneralSource("GoldenDom");
    private GeneralSource goldenDuns = new GeneralSource("GoldenDuns");

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareGoldenDom();
        prepareGoldenDuns();
        prepareSeed();
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
        configuration.setName("SeedDomainDunsCleanup");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(seed.getSourceName());
        baseSources.add(goldenDom.getSourceName());
        baseSources.add(goldenDuns.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(SeedDomainDunsCleanup.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        String confParamStr1 = getTransformerConfig();
        step1.setConfiguration(confParamStr1);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getTransformerConfig() {
        SeedDomainDunsCleanupConfig config = new SeedDomainDunsCleanupConfig();
        config.setGoldenDomainField("Domain");
        config.setGoldenDunsField("DUNS");
        config.setSeedDomainField("Domain");
        config.setSeedDunsField("DUNS");
        return JsonUtils.serialize(config);
    }

    private void prepareGoldenDom() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));

        Object[][] data = new Object[][] { //
                { "a.com", "1" }, //
                { "b.com", "1" }, //
                { "c.com", "1" }, //
                { "c.com", "2" }, //
                { null, null }, //
                { null, null }, //
        };

        uploadBaseSourceData(goldenDom.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareGoldenDuns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("Domain", String.class));

        Object[][] data = new Object[][] { //
                { "2", "d.com" }, //
                { "3", "d.com", }, //
                { "4", "d.com", }, //
                { "4", "e.com", }, //
                { null, null }, //
                { null, null }, //
        };

        uploadBaseSourceData(goldenDuns.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", String.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));

        Object[][] data = new Object[][] { //
                { "1", "a.com", "1" }, // remove
                { "2", "e.com", "1" }, // retain
                { "3", "c.com", "1" }, // remove
                { "4", "c.com", "2" }, // remove
                { "5", "f.com", "4" }, // remove
                { "6", "b.com", "4" }, // remove
                { "7", "f.com", "1" }, // retain
                { "8", "g.com", "5" }, // retain
                { "9", null, "5" }, // retain
                { "10", "g.com", null }, // retain
        };

        uploadBaseSourceData(seed.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        // Schema ID, Domain, DUNS
        Object[][] expectedData = { //
                { "2", "e.com", "1" }, //
                { "7", "f.com", "1" }, //
                { "8", "g.com", "5" }, //
                { "9", null, "5" }, //
                { "10", "g.com", null }, //
        };

        Map<Object, Object[]> map = Arrays.stream(expectedData).collect(Collectors.toMap(x -> (String) x[0], x -> x));

        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = record.get("ID").toString();
            Object[] expectedRecord = map.get(id);
            Assert.assertNotNull(expectedRecord);
            Assert.assertTrue(isObjEquals(record.get("Domain"), expectedRecord[1]));
            Assert.assertTrue(isObjEquals(record.get("DUNS"), expectedRecord[2]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedData.length);
    }
}
