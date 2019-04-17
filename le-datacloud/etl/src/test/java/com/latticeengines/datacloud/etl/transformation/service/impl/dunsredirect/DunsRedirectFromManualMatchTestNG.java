package com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.DunsRedirectFromManualMatch;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.TransformationServiceImplTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectFromManualMatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DunsRedirectFromManualMatchTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final List<Pair<String, Class<?>>> SRC_COLUMNS = new ArrayList<>();
    private static final List<Pair<String, Class<?>>> SINK_COLUMNS = new ArrayList<>();
    // store non-string field since most of the field is String
    private static final Map<String, Class<?>> NON_STR_COLUMN_MAP = new HashMap<>();
    private static final DunsRedirectFromManualMatchConfig CONFIG =
            new DunsRedirectFromManualMatchConfig();

    private GeneralSource source = new GeneralSource("DunsRedirectFromManualMatch");
    private GeneralSource baseSource = new GeneralSource("ManualSeedPostMatchData");

    private AtomicInteger manIdGenerator = new AtomicInteger();

    static {
        // MAN_SalesInBillions is Long & MAN_TotalEmployees is Integer
        NON_STR_COLUMN_MAP.put("MAN_SalesInBillions", Long.class);
        NON_STR_COLUMN_MAP.put("MAN_TotalEmployees", Integer.class);

        addColumns(SRC_COLUMNS, "MAN_Id", "MAN_DUNS", "MAN_CompanyName", "MAN_Country", "MAN_State",
                "MAN_City", "MAN_EmpSize", "MAN_SalesInBillions", "MAN_TotalEmployees", "KeyPartition",
                DataCloudConstants.ATTR_LDC_DUNS);
        addColumns(SINK_COLUMNS, "Duns", "TargetDuns", "KeyPartition", "BookSource");

        CONFIG.setManualSeedDuns("MAN_DUNS");
        CONFIG.setSalesInBillions("MAN_SalesInBillions");
        CONFIG.setTotalEmployees("MAN_TotalEmployees");
        CONFIG.setBookSource("ManualMatch");
    }

    @Test(groups = "pipeline1")
    public void testTransformation() {
        preparePostMatchData();
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
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DunsRedirectFromManualMatch");
        configuration.setVersion(targetVersion);

        // Initialize manualSeed Data Set
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSourceStep = Collections.singletonList(baseSource.getSourceName());
        step.setBaseSources(baseSourceStep);
        step.setTargetSource(source.getSourceName());
        step.setTransformer(DunsRedirectFromManualMatch.TRANSFORMER_NAME);
        step.setConfiguration(JsonUtils.serialize(CONFIG));

        // -----------
        List<TransformationStepConfig> steps = Collections.singletonList(step);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Assert.assertNotNull(records);
        // key = srcDuns + keyPartition, value = targetDuns
        Map<String, Object> resultMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object srcDuns = record.get(DunsRedirectBookConfig.DUNS);
            Object targetDuns = record.get(DunsRedirectBookConfig.TARGET_DUNS);
            Object keyPartition = record.get(DunsRedirectBookConfig.KEY_PARTITION);
            Object bookSource = record.get(DunsRedirectBookConfig.BOOK_SOURCE);
            String key = srcDuns.toString() + keyPartition.toString();
            resultMap.put(key, targetDuns);
            // check redirect book source
            Assert.assertTrue(isObjEquals(bookSource, CONFIG.getBookSource()));
        }

        Object[][] expectedData = provideDunsRedirectBookData();
        Assert.assertEquals(resultMap.size(), expectedData.length);
        for (Object[] row : expectedData) {
            String key = row[0].toString() + row[2].toString();
            // check target DUNS
            Assert.assertTrue(isObjEquals(resultMap.get(key), row[1]));
        }
    }

    private void preparePostMatchData() {
        uploadBaseSourceData(
                baseSource.getSourceName(), baseSourceVersion,
                SRC_COLUMNS, providePostMatchData());
    }

    private Object[][] providePostMatchData() {
        return new Object[][] {
                // #1: null salesInBillion
                newPostMatchData("000000001","987654321", newTuple("USA", "NY"),
                        null, 999),
                newPostMatchData("000000002","987654321", newTuple("USA", "NY"),
                        null, 10000),
                newPostMatchData("000000003","987654321", newTuple("USA", "NY"),
                        null, 12346),

                // #2: equal salesInBillion
                newPostMatchData("000000004","987654321", newTuple("USA", null),
                        123L, 999),
                newPostMatchData("000000005","987654321", newTuple("USA", null),
                        123L, 10000),

                // #3: different salesInBillion
                newPostMatchData("000000006","987654321", newTuple(null, null),
                        99999L, null),
                newPostMatchData("000000007","987654321", newTuple(null, null),
                        123L, 99999999),
                newPostMatchData("000000008","987654321", newTuple(null, null),
                        null, 88888888),

                // #4: same KeyPartition as #1,#2,#3, different ldcDuns
                newPostMatchData("000000009","888888888", newTuple("USA", "NY"),
                        Long.MAX_VALUE, 999),
                newPostMatchData("000000010","777777777", newTuple("USA", null),
                        Long.MAX_VALUE, 999),
                newPostMatchData("000000011","666666666", newTuple(null, null),
                        Long.MAX_VALUE, 999),

                // #5: manDuns == ldcDuns, still generate a redirect book entry to prevent other sources from overriding
                //     and redirect manDuns to other sub-optimal DUNS
                newPostMatchData("444444444","555555555", newTuple(null, null),
                        123L, 999),
                newPostMatchData("555555555","555555555", newTuple(null, null),
                        Long.MAX_VALUE, 999),

                // #6: invalid rows (null ManDuns/LDC_DUNS or ManDuns == LDC_DUNS)
                newPostMatchData(null,null, newTuple("USA", "NY"),
                        Long.MAX_VALUE, 999),
                newPostMatchData("123456789",null, newTuple("USA", null),
                        Long.MAX_VALUE, 999),
                newPostMatchData(null,"987654321", newTuple(null, "NY"),
                        Long.MAX_VALUE, 999),
        };
    }

    private Object[] newPostMatchData(String manDuns, String ldcDuns, MatchKeyTuple tuple,
        Long salesInBillion, Integer totalEmployees) {
        return new Object[] {
                String.valueOf(manIdGenerator.getAndIncrement()),
                manDuns, tuple.getName(), tuple.getCountry(), tuple.getState(), tuple.getCountryCode(),
                "Large", salesInBillion, totalEmployees, MatchKeyUtils.evalKeyPartition(tuple), ldcDuns
        };
    }

    /*
     * each row: [ srcDuns (LDC_DUNS), targetDuns (ManDuns), keyPartition ]
     */
    private Object[][] provideDunsRedirectBookData() {
        return new Object[][] {
                { "987654321", "000000003", getKeyPartition("USA", "NY") }, // #1
                { "987654321", "000000005", getKeyPartition("USA", null) }, // #2
                { "987654321", "000000006", getKeyPartition(null, null) }, // #3
                { "888888888", "000000009", getKeyPartition("USA", "NY") }, // #4
                { "777777777", "000000010", getKeyPartition("USA", null) }, // #4
                { "666666666", "000000011", getKeyPartition(null, null) }, // #4
                { "555555555", "555555555", getKeyPartition(null, null) }, // #5
        };
    }

    private static void addColumns(List<Pair<String, Class<?>>> columns, String... columnNames) {
        for (String name : columnNames) {
            if (NON_STR_COLUMN_MAP.containsKey(name)) {
                columns.add(Pair.of(name, NON_STR_COLUMN_MAP.get(name)));
            } else {
                addStringColumn(columns, name);
            }
        }
    }

    private static void addStringColumn(List<Pair<String, Class<?>>> columns, String columnName) {
        columns.add(Pair.of(columnName, String.class));
    }

    /*
     * Post match data should always has null city field at the moment
     */
    private MatchKeyTuple newTuple(String country, String state) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setName("Wall Street Chinese");
        tuple.setCountryCode(country);
        tuple.setCountry(country);
        tuple.setState(state);
        return tuple;
    }

    private String getKeyPartition(String country, String state) {
        return MatchKeyUtils.evalKeyPartition(newTuple(country, state));
    }
}
