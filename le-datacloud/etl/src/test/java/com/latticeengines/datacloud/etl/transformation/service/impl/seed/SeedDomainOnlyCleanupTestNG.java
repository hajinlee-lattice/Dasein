package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.seed.SeedDomainOnlyCleanup;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.SeedDomainOnlyCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SeedDomainOnlyCleanupTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(SeedDomainOnlyCleanupTestNG.class);

    private GeneralSource seed = new GeneralSource("Seed");
    private GeneralSource src1 = new GeneralSource("Src1");
    private GeneralSource src2 = new GeneralSource("Src2");
    private GeneralSource src3 = new GeneralSource("Src3");
    private GeneralSource source = new GeneralSource("CleanSeed");

    private static final String ID = "ID";
    private static final String DOMAIN = "Domain";
    private static final String URL = "URL";
    private static final String DUNS = "Duns";

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareSeed();
        prepareSrc1();
        prepareSrc2();
        prepareSrc3();
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
        configuration.setName("SeedDomainOnlyCleanup");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(seed.getSourceName());
        baseSources.add(src1.getSourceName());
        baseSources.add(src2.getSourceName());
        baseSources.add(src3.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(SeedDomainOnlyCleanup.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getSeedDomainOnlyCleanup());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getSeedDomainOnlyCleanup() {
        SeedDomainOnlyCleanupConfig config = new SeedDomainOnlyCleanupConfig();
        config.setSeedDomain(DOMAIN);
        config.setSeedDuns(DUNS);
        Map<String, String> srcDomains = ImmutableMap.of( //
                src1.getSourceName(), URL, //
                src2.getSourceName(), DOMAIN, //
                src3.getSourceName(), DOMAIN);
        config.setSrcDomains(srcDomains);
        return JsonUtils.serialize(config);
    }

    // ID format: <CaseId>_XXX. CaseId is used in result verification
    private Object[][] seedData = new Object[][] {
            // Case 1: Non-DomainOnly entries -- Retain
            { "C01_01", "src1_dom.com", "duns1" }, //
            { "C01_02", "src2_dom.com", "duns2" }, //
            { "C01_03", "src3_dom.com", "duns2" }, //
            { "C01_04", "nosrc_dom.com", "duns1" }, //
            { "C01_05", null, "duns2" }, //

            // Case 2: DomainOnly entries match to at least one source --
            // Retain
            { "C02_01", "src1_dom.com", null }, //
            { "C02_02", "src2_dom.com", null }, //
            { "C02_03", "src3_dom.com", null }, //
            { "C02_04", "src12_dom.com", null }, //
            { "C02_05", "src13_dom.com", null }, //
            { "C02_06", "src23_dom.com", null }, //
            { "C02_07", "src123_dom.com", null }, //

            // Case 3: DomainOnly entries cannot match to any source --
            // Remove
            { "C03_01", "nosrc_dom.com", null }, //
            { "C03_02", "nosrc_dom1.com", null }, //
    };

    private void prepareSeed() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(ID, String.class));
        schema.add(Pair.of(DOMAIN, String.class));
        schema.add(Pair.of(DUNS, String.class));
        uploadBaseSourceData(seed.getSourceName(), baseSourceVersion, schema, seedData);
    }

    private void prepareSrc1() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(ID, String.class));
        schema.add(Pair.of(URL, String.class));
        Object[][] data = new Object[][] { //
                { "1", "src1_dom.com" }, //
                { "2", "src12_dom.com" }, //
                { "3", "src13_dom.com" }, //
                { "4", "src123_dom.com" }, //
        };
        uploadBaseSourceData(src1.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareSrc2() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(ID, String.class));
        schema.add(Pair.of(DOMAIN, String.class));
        Object[][] data = new Object[][] { //
                { "1", "src2_dom.com" }, //
                { "2", "src12_dom.com" }, //
                { "3", "src23_dom.com" }, //
                { "4", "src123_dom.com" }, //
        };
        uploadBaseSourceData(src2.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareSrc3() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(ID, String.class));
        schema.add(Pair.of(DOMAIN, String.class));
        Object[][] data = new Object[][] { //
                { "1", "src3_dom.com" }, //
                { "2", "src13_dom.com" }, //
                { "3", "src23_dom.com" }, //
                { "4", "src123_dom.com" }, //
        };
        uploadBaseSourceData(src3.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Set<String> removedCases = new HashSet<>(Arrays.asList("C03"));
        Map<String, Object[]> expectedSeed = Arrays.stream(seedData)
                .filter(data -> !removedCases.contains(parseCaseId((String) data[0])))
                .collect(Collectors.toMap(data -> (String) data[0], data -> data));
        int total = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = record.get(ID).toString();
            Assert.assertTrue(expectedSeed.containsKey(id));
            isObjEquals(record.get(DOMAIN), expectedSeed.get(id)[1]);
            isObjEquals(record.get(DUNS), expectedSeed.get(id)[2]);
            total++;
        }
        Assert.assertEquals(total, expectedSeed.size());
    }

    private String parseCaseId(String recordId) {
        return recordId.split("_")[0];
    }

}
