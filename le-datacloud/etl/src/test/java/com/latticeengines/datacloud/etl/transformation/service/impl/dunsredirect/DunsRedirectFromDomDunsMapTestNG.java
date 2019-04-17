package com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect;

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

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.DunsRedirectFromDomDunsMap;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsRedirectFromDomDunsMapTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DunsRedirectFromDomDunsMapTestNG.class);

    private GeneralSource ams = new GeneralSource("AccountMasterSeed");
    private GeneralSource dunsRedirect = new GeneralSource("DunsRedirectBook_DomDunsMap");
    private GeneralSource source = dunsRedirect;

    private static final String BOOK_SOURCE = "DomainDunsMap";

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareData();
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
        configuration.setName("BuildDunsRedirectBookFromDomDunsMap");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(ams.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DunsRedirectFromDomDunsMap.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getDunsRedirectBookConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getDunsRedirectBookConfig() {
        DunsRedirectBookConfig config = new DunsRedirectBookConfig();
        config.setBookSource(BOOK_SOURCE);
        return JsonUtils.serialize(config);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.LATTICE_ID, Long.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_COUNTRY, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_STATE, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION, String.class));

        Object[][] data = new Object[][] { //
                // Duns-only entry, no duns redirect
                { 1L, null, "Duns01", "Ctry01", "St01", "Y", "Y", "Y" }, //
                // Domain-only entry, no duns redirect
                { 2L, "Dom02", null, "Ctry02", "St02", null, null, null }, //
                { 3L, "Dom03", null, "Ctry03", "St03", "N", "N", "N" }, //
                // Normal case
                { 4L, "Dom11", "Duns11", "Ctry11", "St11", "Y", "Y", "Y" }, //
                { 5L, "Dom11", "Duns12", "Ctry11", "St11", "N", "N", "N" }, //
                { 6L, "Dom11", "Duns13", "Ctry13", "St13", "N", "Y", "Y" }, //
                { 7L, "Dom11", "Duns14", "Ctry13", "St13", "N", "N", "N" }, //
                { 8L, "Dom11", "Duns15", "Ctry13", "St15", "N", "N", "Y" }, //
                { 9L, "Dom11", "Duns16", "Ctry13", "St15", "N", "N", "N" }, //
                // Empty country or state, no duns redirect for country/state
                // key partition
                { 10L, "Dom21", "Duns21", null, null, "Y", "Y", "Y" }, //
                { 11L, "Dom21", "Duns22", null, null, "N", "N", "N" }, //
        };
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // Schema: Duns, TargetDuns, KeyPartition
        // Duns + KeyPartition should be unique
        String[][] expected = { //
                { "Duns12", "Duns11", "Name" }, //
                { "Duns12", "Duns11", "Country,Name" }, //
                { "Duns12", "Duns11", "Country,Name,State" }, //
                { "Duns13", "Duns11", "Name" }, //
                { "Duns14", "Duns11", "Name" }, //
                { "Duns14", "Duns13", "Country,Name" }, //
                { "Duns14", "Duns13", "Country,Name,State" }, //
                { "Duns15", "Duns11", "Name" }, //
                { "Duns15", "Duns13", "Country,Name" }, //
                { "Duns16", "Duns11", "Name" }, //
                { "Duns16", "Duns13", "Country,Name" }, //
                { "Duns16", "Duns15", "Country,Name,State" }, //
                { "Duns22", "Duns21", "Name" }, //
        };
        Map<String, String[]> map = new HashMap<>();
        for (String[] item : expected) {
            map.put(buildId(item[0], item[2]), item);
        }
        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            cnt++;
            String id = buildId(record.get(DunsRedirectBookConfig.DUNS).toString(),
                    record.get(DunsRedirectBookConfig.KEY_PARTITION).toString());
            Assert.assertNotNull(map.get(id));
            String[] expectedRes = map.get(id);
            Assert.assertEquals(record.get(DunsRedirectBookConfig.TARGET_DUNS).toString(), expectedRes[1]);
            Assert.assertEquals(record.get(DunsRedirectBookConfig.BOOK_SOURCE).toString(), BOOK_SOURCE);
        }
        Assert.assertEquals(cnt, expected.length);
    }

    private String buildId(String duns, String keyPartition) {
        return duns + "_" + keyPartition;
    }
}
