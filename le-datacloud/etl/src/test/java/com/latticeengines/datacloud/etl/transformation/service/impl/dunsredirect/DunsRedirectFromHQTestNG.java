package com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.DunsRedirectFromHQ;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsRedirectFromHQTestNG extends PipelineTransformationTestNGBase {
    private static final String BOOK_SOURCE_VALUE = "DunsTree";
    private static final String KEY_PARTITION_VALUE = getKeyPartitionValue();

    private static final List<Pair<String, Class<?>>> SRC_COLUMNS = new ArrayList<>();
    private static final DunsRedirectBookConfig CONFIG = new DunsRedirectBookConfig();

    private GeneralSource source = new GeneralSource("DunsRedirectFromHQData");
    private GeneralSource baseSource = new GeneralSource("AccountMasterSeedData");

    static {
        SRC_COLUMNS.add(Pair.of(DataCloudConstants.ATTR_LDC_DUNS, String.class));
        SRC_COLUMNS.add(Pair.of(DataCloudConstants.ATTR_GU_DUNS, String.class));
        SRC_COLUMNS.add(Pair.of(DataCloudConstants.ATTR_SALES_VOL_US, Long.class));
        SRC_COLUMNS.add(Pair.of(DataCloudConstants.ATTR_GLOBAL_HQ_SALES_VOL, Long.class));

        CONFIG.setBookSource(BOOK_SOURCE_VALUE);
    }

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAccountMasterSeedData();
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
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DunsRedirectFromHQ");
        configuration.setVersion(targetVersion);

        // Initialize manualSeed Data Set
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSourceStep = Collections.singletonList(baseSource.getSourceName());
        step.setBaseSources(baseSourceStep);
        step.setTargetSource(source.getSourceName());
        step.setTransformer(DunsRedirectFromHQ.TRANSFORMER_NAME);
        step.setConfiguration(JsonUtils.serialize(CONFIG));

        // -----------
        List<TransformationStepConfig> steps = Collections.singletonList(step);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // key = srcDuns, value = targetDuns
        Map<String, String> expectedDunsMap = getDunsMap(provideDunsRedirectBookData());
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String srcDuns = record.get(DunsRedirectBookConfig.DUNS).toString();
            String targetDuns = record.get(DunsRedirectBookConfig.TARGET_DUNS).toString();
            String keyPartition = record.get(DunsRedirectBookConfig.KEY_PARTITION).toString();
            String bookSource = record.get(DunsRedirectBookConfig.BOOK_SOURCE).toString();

            // check srcDuns & targetDuns
            Assert.assertTrue(expectedDunsMap.containsKey(srcDuns));
            Assert.assertTrue(isObjEquals(targetDuns, expectedDunsMap.get(srcDuns)));
            // check keyPartition & bookSource
            Assert.assertEquals(keyPartition, KEY_PARTITION_VALUE);
            Assert.assertEquals(bookSource, BOOK_SOURCE_VALUE);

            // remove entry to make sure no duplicate srcDuns
            expectedDunsMap.remove(srcDuns);
        }
        // make sure all expected entry is returned
        Assert.assertTrue(expectedDunsMap.isEmpty());
    }

    private void prepareAccountMasterSeedData() {
        uploadBaseSourceData(
                baseSource.getSourceName(), baseSourceVersion,
                SRC_COLUMNS, provideAccountMasterSeedData());
    }

    /*
     * each row: [ LDC_DUNS, GU_DUNS, SALES_VOL_US, GLOBAL_HQ_SALES_VOL ]
     */
    private Object[][] provideAccountMasterSeedData() {
        return new Object[][] {
                // #1: duplicate rows, all valid sale vol
                { "123456789", "999999999", 12345L, Long.MAX_VALUE },
                { "123456789", "999999999", 0L, 1L },
                { "123456789", "999999999", null, 5L },
                { "123456789", "999999999", null, null },

                // #2: single row, valid sales vol
                { "000000001", "100000001", 12345L, Long.MAX_VALUE },
                { "000000002", "100000002", 0L, 1L },
                { "000000003", "100000003", 999L, 999L },
                { "000000004", "100000004", null, 5L },
                { "000000005", "100000005", null, null },

                // #3: invalid rows
                { "888888888", null, 12345L, Long.MAX_VALUE },
                { null, "888888888", 12345L, Long.MAX_VALUE },
                { null, null, 12345L, Long.MAX_VALUE },
                { "999999999", "999999999", 12345L, Long.MAX_VALUE }, // LDC_DUNS == GU_DUNS
                { "666666666", "999999999", 12345L, null }, // SALES_VOL not null, GHQ_SALES_VOL null
                { "777777777", "999999999", 12345L, 1234L }, // SALES_VOL > GHQ_SALES_VOL
        };
    }

    /*
     * each row: [ srcDuns (LDC_DUNS), targetDuns (GU_DUNS) ]
     */
    private Object[][] provideDunsRedirectBookData() {
        return new Object[][]{
                { "123456789", "999999999" }, // #1
                { "000000001", "100000001" }, // #2
                { "000000002", "100000002" }, // #2
                { "000000003", "100000003" }, // #2
                { "000000004", "100000004" }, // #2
                { "000000005", "100000005" }, // #2
        };
    }

    /*
     * helper to generate srcDuns => targetDuns map from the result of provideDunsRedirectBookData()
     */
    private Map<String, String> getDunsMap(Object[][] dunsRedirectBookData) {
        return Arrays
                .stream(dunsRedirectBookData)
                .collect(Collectors.toMap(row -> (String) row[0], row -> (String) row[1]));
    }


    private static String getKeyPartitionValue() {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setName(DataCloudConstants.ATTR_LDC_NAME);
        return MatchKeyUtils.evalKeyPartition(tuple);
    }
}
