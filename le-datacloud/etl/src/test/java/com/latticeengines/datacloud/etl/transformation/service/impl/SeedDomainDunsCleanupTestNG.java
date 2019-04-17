package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.SeedDomainDunsCleanup;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SeedDomainDunsCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;


public class SeedDomainDunsCleanupTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(SeedDomainDunsCleanupTestNG.class);

    GeneralSource source = new GeneralSource("SeedCleaned");
    GeneralSource seed = new GeneralSource("Seed");
    GeneralSource goldenDom = new GeneralSource("GoldenDom");
    GeneralSource goldenDuns = new GeneralSource("GoldenDuns");

    ObjectMapper om = new ObjectMapper();

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

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("SeedDomainDunsCleanup");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(seed.getSourceName());
            baseSources.add(goldenDom.getSourceName());
            baseSources.add(goldenDuns.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SeedDomainDunsCleanup.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfig() throws JsonProcessingException {
        SeedDomainDunsCleanupConfig config = new SeedDomainDunsCleanupConfig();
        config.setGoldenDomainField("Domain");
        config.setGoldenDunsField("DUNS");
        config.setSeedDomainField("Domain");
        config.setSeedDunsField("DUNS");
        return om.writeValueAsString(config);
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
        Object[][] expectedData = { //
                { "2", "e.com", "1" }, //
                { "7", "f.com", "1" }, //
                { "8", "g.com", "5" }, //
                { "9", null, "5" }, //
                { "10", "g.com", null }, //
        };

        Map<Object, Object[]> map = new HashMap<>();
        for (Object[] data : expectedData) {
            map.put(data[0], data);
        }

        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object id = record.get("ID");
            if (id instanceof Utf8) {
                id = id.toString();
            }
            Object[] data = map.get(id);
            Assert.assertNotNull(data);
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            Assert.assertEquals(domain, data[1]);
            Object duns = record.get("DUNS");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Assert.assertEquals(duns, data[2]);
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedData.length);
    }
}
