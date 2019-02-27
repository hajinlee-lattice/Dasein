package com.latticeengines.datacloud.etl.transformation.service.impl.dunsredirect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.dunsredirect.PrepareDunsRedirectManualMatch;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.TransformationServiceImplTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PrepareDunsRedirectManualMatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class PrepareDunsRedirectManualMatchTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final List<Pair<String, Class<?>>> SRC_COLUMNS = new ArrayList<>();
    private static final List<Pair<String, Class<?>>> SINK_COLUMNS = new ArrayList<>();
    private static final Map<String, Integer> SRC_FIELD_INDEX_MAP;
    // store non-string field since most of the field is String
    private static final Map<String, Class<?>> NON_STR_COLUMN_MAP = new HashMap<>();
    private static final PrepareDunsRedirectManualMatchConfig CONFIG = new PrepareDunsRedirectManualMatchConfig();

    private static final String EMPLOYEE_SIZE_LARGE =
            PrepareDunsRedirectManualMatchConfig.LARGE_COMPANY_EMPLOYEE_SIZE;
    private static final String EMPLOYEE_SIZE_SMALL =
            PrepareDunsRedirectManualMatchConfig.SMALL_COMPANY_EMPLOYEE_SIZE;

    static {
        // MAN_SalesInBillions is Long & MAN_TotalEmployees is Integer
        NON_STR_COLUMN_MAP.put("MAN_SalesInBillions", Long.class);
        NON_STR_COLUMN_MAP.put("MAN_TotalEmployees", Integer.class);

        // populate source column names
        addColumns(SRC_COLUMNS, "MAN_Id", "MAN_FixDUTreeFlag", "MAN_Domain", "MAN_CompanyName",
                "MAN_Street", "MAN_City", "MAN_State", "MAN_Country", "MAN_DUNS", "MAN_PostalCode",
                "MAN_SalesInBillions", "MAN_Industry", "MAN_TotalEmployees", "MAN_CEO1stName", "MAN_CEOLastName",
                "MAN_CEOTitle", "MAN_EmpSize", "MAN_LE_EMPLOYEE_RANGE", "MAN_LE_REVENUE_RANGE");
        addColumns(SINK_COLUMNS, "MAN_Id", "MAN_DUNS", "MAN_CompanyName", "MAN_Country", "MAN_State",
                "MAN_City", "MAN_EmpSize", "MAN_SalesInBillions", "MAN_TotalEmployees",
                DunsRedirectBookConfig.KEY_PARTITION);
        SRC_FIELD_INDEX_MAP = IntStream.range(0, SRC_COLUMNS.size())
                .mapToObj(idx -> Pair.of(SRC_COLUMNS.get(idx).getKey(), idx))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // generate transformer config
        CONFIG.setManId("MAN_Id");
        CONFIG.setDuns("MAN_DUNS");
        CONFIG.setCompanyName("MAN_CompanyName");
        CONFIG.setCountry("MAN_Country");
        CONFIG.setState("MAN_State");
        CONFIG.setCity("MAN_City");
        CONFIG.setEmployeeSize("MAN_EmpSize");
        CONFIG.setSalesInBillions("MAN_SalesInBillions");
        CONFIG.setTotalEmployees("MAN_TotalEmployees");
    }

    private GeneralSource source = new GeneralSource("PrepareDunsRedirectManualMatchData");
    private GeneralSource baseSource = new GeneralSource("ManualSeedStandardData");

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareManualSeedStandardData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareManualSeedStandardData() {
        uploadBaseSourceData(
                baseSource.getSourceName(), baseSourceVersion,
                SRC_COLUMNS, provideManualSeedStandardTestData());
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
        configuration.setName("PrepareDunsRedirectManualMatch");
        configuration.setVersion(targetVersion);

        // Initialize manualSeed Data Set
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSourceStep = Collections.singletonList(baseSource.getSourceName());
        step1.setBaseSources(baseSourceStep);
        step1.setTargetSource(source.getSourceName());
        step1.setTransformer(PrepareDunsRedirectManualMatch.TRANSFORMER_NAME);
        step1.setConfiguration(JsonUtils.serialize(CONFIG));

        // -----------
        List<TransformationStepConfig> steps = Collections.singletonList(step1);

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
        Object[][] expectedData = provideExpectedData();
        int idx = 0;
        for (; records.hasNext(); idx++) {
            GenericRecord record = records.next();
            Assert.assertTrue(idx < expectedData.length);
            for (int j = 0; j < SINK_COLUMNS.size(); j++) {
                String name = SINK_COLUMNS.get(j).getKey();
                Object obj = record.get(name);
                Assert.assertTrue(isObjEquals(obj, expectedData[idx][j]));
            }
        }
        // number of rows match
        Assert.assertEquals(idx, expectedData.length);
    }

    private Object[][] provideManualSeedStandardTestData() {
        return new Object[][] {
                // #1: all name/location fields present, large & small company size
                newManualSeedStandardRow("1", "Wall Street Chinese", "USA",
                        "NY", "New York", EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("2", "Wall Street Chinese", "USA",
                        "NY", "New York", EMPLOYEE_SIZE_SMALL),

                // #2: name, country, state present, large & small company size
                newManualSeedStandardRow("3", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("4", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_SMALL),

                // #3: name, country, city present, large & small company size
                newManualSeedStandardRow("5", "Wall Street Chinese", "USA",
                        null, "New York", EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("6", "Wall Street Chinese", "USA",
                        null, "New York", EMPLOYEE_SIZE_SMALL),

                // #4: name, country present, large & small company size
                newManualSeedStandardRow("7", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("8", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_SMALL),

                // #5: name present, large & small company size
                newManualSeedStandardRow("9", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("10", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),

                /* invalid rows, will not generate any output rows */

                // #6: invalid employee size
                newManualSeedStandardRow("11", "Wall Street Chinese", "USA",
                        "NY", "New York", null),
                newManualSeedStandardRow("12", "Wall Street Chinese", "USA",
                        "NY", "New York", "Medium"),

                // #7: no company name
                newManualSeedStandardRow("13", null, "USA",
                        "NY", "New York", EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("14", null, "USA",
                        "NY", null, EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("15", null, "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),
                newManualSeedStandardRow("16", null, null,
                        null, null, EMPLOYEE_SIZE_LARGE),

        };
    }

    private Object[][] provideExpectedData() {
        return new Object[][] {
                // #1: all name/location fields present, large & small company size
                newExpectedRow("1", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("1", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("1", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_LARGE),

                newExpectedRow("2", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("2", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("2", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("2", "Wall Street Chinese", "USA",
                        null, "New York", EMPLOYEE_SIZE_SMALL),
                newExpectedRow("2", "Wall Street Chinese", "USA",
                        "NY", "New York", EMPLOYEE_SIZE_SMALL),

                // #2: name, country, state present, large & small company size
                newExpectedRow("3", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("3", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("3", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_LARGE),

                newExpectedRow("4", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("4", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("4", "Wall Street Chinese", "USA",
                        "NY", null, EMPLOYEE_SIZE_SMALL),

                // #3: name, country, city present, large & small company size
                newExpectedRow("5", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("5", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),

                newExpectedRow("6", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("6", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("6", "Wall Street Chinese", "USA",
                        null, "New York", EMPLOYEE_SIZE_SMALL),

                // #4: name, country present, large & small company size
                newExpectedRow("7", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),
                newExpectedRow("7", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_LARGE),

                newExpectedRow("8", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),
                newExpectedRow("8", "Wall Street Chinese", "USA",
                        null, null, EMPLOYEE_SIZE_SMALL),

                // #5: name present, large & small company size
                newExpectedRow("9", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_LARGE),

                newExpectedRow("10", "Wall Street Chinese", null,
                        null, null, EMPLOYEE_SIZE_SMALL),
        };
    }

    /* helper methods */

    private Object[] newManualSeedStandardRow(String id, String companyName, String country,
        String state, String city, String employeeSize) {
        return new Object[] {
                id, "N", "www.lattice-engines.com", companyName, "street", city, state, country, "DUNS", "PostalCode",
                10L, "Industry", 9527, null, null, null, employeeSize, null, null
        };
    }

    private Object[] newExpectedRow(String id, String companyName, String country,
        String state, String city, String employeeSize) {
        Object[] row = newManualSeedStandardRow(id, companyName, country, state, city, employeeSize);
        Object[] result = new Object[SINK_COLUMNS.size()];
        // only sink columns will be retained
        for (int i = 0; i < SINK_COLUMNS.size() - 1; i++) {
            result[i] = row[SRC_FIELD_INDEX_MAP.get(SINK_COLUMNS.get(i).getKey())];
        }
        result[SINK_COLUMNS.size() - 1] = MatchKeyUtils.evalKeyPartition(newTuple(country, state, city));
        return result;
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

    private MatchKeyTuple newTuple(String country, String state, String city) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        // always have name at the moment
        tuple.setName("CompanyName");
        tuple.setCountryCode(country);
        tuple.setState(state);
        tuple.setCity(city);
        return tuple;
    }
}
