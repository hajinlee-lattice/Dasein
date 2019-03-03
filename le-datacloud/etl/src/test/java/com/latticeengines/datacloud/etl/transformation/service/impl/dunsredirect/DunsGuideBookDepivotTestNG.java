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

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.DunsGuideBookDepivot;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsGuideBookDepivotTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookDepivotTestNG.class);

    private GeneralSource dunsGuideBook = new GeneralSource("DunsGuideBook");
    private GeneralSource ams = new GeneralSource("AccountMasterSeed");
    private GeneralSource dunsGuideBookDepivoted = new GeneralSource("DunsGuideBookDepivoted");
    private GeneralSource source = dunsGuideBookDepivoted;

    private static final String DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String DU_DUNS = DunsGuideBook.SRC_DU_DUNS_KEY;
    private static final String GU_DUNS = DunsGuideBook.SRC_GU_DUNS_KEY;
    private static final String ITEMS = DunsGuideBook.ITEMS_KEY;

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareDunsGuideBook();
        prepareAMS();
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
        configuration.setName("DunsGuideBookDepivot");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(dunsGuideBook.getSourceName());
        baseSources.add(ams.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DunsGuideBookDepivot.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration("{}");

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private void prepareDunsGuideBook() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DUNS, String.class));
        schema.add(Pair.of(DU_DUNS, String.class));
        schema.add(Pair.of(GU_DUNS, String.class));
        schema.add(Pair.of(ITEMS, String.class));

        // Schema: Duns, Items
        Object[][] data = new Object[][] { //
                // Case 1: null Items
                { "DUNS01", "DUDUNS01", "GUDUNS01", null }, //
                // Case 2: Items is empty list
                { "DUNS02", "DUDUNS02", "GUDUNS02", "[]" }, //
                // Case 3: Items list has single item
                { "DUNS03", "DUDUNS03", "GUDUNS03",
                        "[{\"TargetDuns\":\"TDUNS3\",\"TargetDUDuns\":\"TDUDUNS3\",\"TargetGUDuns\":\"TGUDUNS3\",\"KeyPartition\":\"Country,Name,State\",\"BookSource\":\"DomDunsMap\"}]" }, //
                // Case 4: Items list has multiple items. Case 3 & 4 covers all
                // the possible KeyPartition
                { "DUNS04", "DUDUNS04", "GUDUNS04",
                        "[{\"TargetDuns\":\"TDUNS4\",\"TargetDUDuns\":\"TDUDUNS4\",\"TargetGUDuns\":\"TGUDUNS4\",\"KeyPartition\":\"Country,Name\",\"BookSource\":\"ManualMatch\"},"
                                + "{\"TargetDuns\":\"TDUNS44\",\"TargetDUDuns\":\"TDUDUNS44\",\"TargetGUDuns\":\"TGUDUNS44\",\"KeyPartition\":\"Name\",\"BookSource\":\"DunsTree\"},"
                                + "{\"TargetDuns\":\"TDUNS444\",\"TargetDUDuns\":\"TDUDUNS444\",\"TargetGUDuns\":\"TGUDUNS444\",\"KeyPartition\":\"City,Country,Name,State\",\"BookSource\":\"DomDunsMap\"}]" }, //
        };
        uploadBaseSourceData(dunsGuideBook.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareAMS() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.LATTICE_ID, Long.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LDC_NAME, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_COUNTRY, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_STATE, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_CITY, String.class));

        // Schema: LatticeID, Domain, Duns, Name, Country, State, City
        Object[][] data = new Object[][] { //
                { 1L, "dom01.com", "DUNS01", "Name01", "Country01", "State01", "City01" }, //
                { 2L, "dom02.com", "DUNS02", "Name02", "Country02", "State02", "City02" }, //
                { 3L, "dom03.com", "DUNS03", "Name03", "Country03", "State03", "City03" }, //
                // duplicate duns in ams should not introduce duplicate duns in
                // target source
                { 4L, "dom04_1.com", "DUNS04", "Name04", "Country04", "State04", "City04" }, //
                { 5L, "dom04_2.com", "DUNS04", "Name04", "Country04", "State04", "City04" }, //
        };
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // Schema: Duns, TargetDuns, KeyPartition, BookSource, Name, Country,
        // State, City
        Object[][] expectedItems = new Object[][] { //
                { "DUNS03", "TDUNS3", "Country,Name,State", "DomDunsMap", "Name03", "Country03", "State03", null }, //
                { "DUNS04", "TDUNS4", "Country,Name", "ManualMatch", "Name04", "Country04", null, null }, //
                { "DUNS04", "TDUNS44", "Name", "DunsTree", "Name04", null, null, null }, //
                { "DUNS04", "TDUNS444", "City,Country,Name,State", "DomDunsMap", "Name04", "Country04", "State04",
                        "City04" } //
        };
        Map<String, Object[]> expectedMap = new HashMap<>();
        for (Object[] item : expectedItems) {
            expectedMap.put(buildId(item[0], item[2]), item);
        }
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = buildId(record.get(DUNS), record.get(DunsRedirectBookConfig.KEY_PARTITION));
            Object[] expected = expectedMap.get(id);
            Assert.assertNotNull(expected);
            Assert.assertTrue(isObjEquals(record.get(DUNS), expected[0]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.TARGET_DUNS), expected[1]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.KEY_PARTITION), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.BOOK_SOURCE), expected[3]));
            Assert.assertTrue(isObjEquals(record.get(MatchKey.Name.name()), expected[4]));
            Assert.assertTrue(isObjEquals(record.get(MatchKey.Country.name()), expected[5]));
            Assert.assertTrue(isObjEquals(record.get(MatchKey.State.name()), expected[6]));
            Assert.assertTrue(isObjEquals(record.get(MatchKey.City.name()), expected[7]));
            count++;
        }
        Assert.assertEquals(count, expectedItems.length);
    }

    private String buildId(Object duns, Object keyPartition) {
        return String.valueOf(duns) + String.valueOf(keyPartition);
    }
}
