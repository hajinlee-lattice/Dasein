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
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsGuideBookDepivotTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookDepivotTestNG.class);

    private GeneralSource dunsGuideBook = new GeneralSource("DunsGuideBook");
    private GeneralSource dunsGuideBookDepivoted = new GeneralSource("DunsGuideBookDepivoted");
    private GeneralSource source = dunsGuideBookDepivoted;

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
        configuration.setName("DunsGuideBookDepivot");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(dunsGuideBook.getSourceName());
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

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DunsGuideBookConfig.DUNS, String.class));
        schema.add(Pair.of(DunsGuideBookConfig.ITEMS, String.class));

        // Schema: Duns, Items
        Object[][] data = new Object[][] { //
                { "DUNS1", null }, //
                { "DUNS2", "[]" }, //
                { "DUNS3",
                        "[{\"TargetDuns\":\"TDUNS3\",\"KeyPartition\":\"Country,Name,State\",\"BookSource\":\"DomDunsMap\"}]" }, //
                { "DUNS4",
                        "[{\"TargetDuns\":\"TDUNS4\",\"KeyPartition\":\"Country,Name\",\"BookSource\":\"ManualMatch\"},{\"TargetDuns\":\"TDUNS44\",\"KeyPartition\":\"Name\",\"BookSource\":\"DunsTree\"}]" }, //
        };
        uploadBaseSourceData(dunsGuideBook.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // Schema: Duns, TargetDuns, KeyPartition, BookSource
        Object[][] expectedItems = new Object[][] { //
                { "DUNS3", "TDUNS3", "Country,Name,State", "DomDunsMap" }, //
                { "DUNS4", "TDUNS4", "Country,Name", "ManualMatch" }, //
                { "DUNS4", "TDUNS44", "Name", "DunsTree" } //
        };
        Map<String, Object[]> expectedMap = new HashMap<>();
        for (Object[] item : expectedItems) {
            expectedMap.put(buildId(item[0], item[2]), item);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = buildId(record.get(DunsGuideBookConfig.DUNS), record.get(DunsRedirectBookConfig.KEY_PARTITION));
            Object[] expected = expectedMap.get(id);
            Assert.assertTrue(isObjEquals(record.get(DunsGuideBookConfig.DUNS), expected[0]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.TARGET_DUNS), expected[1]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.KEY_PARTITION), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(DunsRedirectBookConfig.BOOK_SOURCE), expected[3]));
        }
    }

    private String buildId(Object duns, Object keyPartition) {
        return String.valueOf(duns) + String.valueOf(keyPartition);
    }
}
