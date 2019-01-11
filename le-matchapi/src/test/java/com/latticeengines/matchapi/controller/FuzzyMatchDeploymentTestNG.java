package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.exposed.util.TestDunsGuideBookUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

@Component
public class FuzzyMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FuzzyMatchDeploymentTestNG.class);;

    private static final String invalidDomain = "abcdefghijklmn.com";
    private static final String podId = "FuzzyMatchDeploymentTestNG";
    private static final String avroDir = "/tmp/FuzzyMatchDeploymentTestNG";
    private static final String DUNS_GUIDE_BOOK_DATACLOUD_VERSION = "2.0.14";
    private static final String DECISION_GRAPH_WITHOUT_GUIDE_BOOK = "Gingerbread";
    private static final String DECISION_GRAPH_WITH_GUIDE_BOOK = "Honeycomb";
    private static final String DUNS_GUIDE_BOOK_DIR = "DunsGuideBook";
    private static final String DUNS_GUIDE_BOOK_TEST_FILE = "dunsguidebookdata.avro";

    // For realtime match: the first element is expected matched company name
    private static final Object[][] nameLocation = {
            { "Benchmark Blinds", "BENCHMARK BLINDS", "GILBERT", "ARIZONA", "United States", "4809857961", "85233" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", "America", "6502530000", "94043" } };

    // For realtime match: the first element is expected matched company name
    private static final Object[][] nameLocationIncomplete = { { null, "", "MOUNTAIN VIEW", "CA", "America" },
            { null, null, "MOUNTAIN VIEW", "CA", "America" },
            { null, " +     * = # ", "MOUNTAIN VIEW", "CA", "America" },
            { "Google Inc.", "GOOGLE", "", "CA", "America" }, { "Google Inc.", "GOOGLE", null, "CA", "America" },
            { "Google Inc.", "GOOGLE", " +     * = # ", "CA", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", null, "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", " +     * = # ", "America" },
            { "Google Inc.", "GOOGLE", "", "", "America" }, { "Google Inc.", "GOOGLE", null, null, "America" },
            { "Google Inc.", "GOOGLE", " +     * = # ", " +     * = # ", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", "" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", null },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", " +     * = # " } };

    private static final String[] selectedColumns = { "LatticeAccountId", "LDC_Name", DataCloudConstants.ATTR_LDC_DUNS,
            "LDC_Domain", "LDC_Country", "LDC_State", "LDC_City", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "YEAR_STARTED" };

    private static final String SCENARIO_VALIDLOCATION = "ValidLocation";
    private static final String SCENARIO_VALIDLOCATION_INVALIDDOMAIN = "ValidLocationInvalidDomain";
    private static final String SCENARIO_WITHOUT_NAME = "WithoutName";
    private static final String SCENARIO_WITHOUT_COUNTRY = "WithoutCountry";
    private static final String SCENARIO_WITHOUT_STATE = "WithoutState";
    private static final String SCENARIO_WITHOUT_CITY = "WithoutCity";
    private static final String SCENARIO_WITHOUT_STATE_CITY = "WithoutStateCity";
    private static final String SCENARIO_INCOMPLETELOCATION = "IncompleteLocation";
    private static final String SCENARIO_BULKMATCH_LOADTEST = "BulkMatchLoadTest";
    private static final String SCENARIO_NAME_PHONE = "NamePhone";
    private static final String SCENARIO_NAME_ZIPCODE = "NameZipCode";

    private static final String VALIDLOCATION_FILENAME = "BulkFuzzyMatchInput_ValidLocation.avro";
    private static final String VALIDLOCATION_INVALIDDOMAIN_FILENAME = "BulkFuzzyMatchInput_ValidLocation_InvalidDomain.avro";
    private static final String WITHOUT_NAME_FILENAME = "BulkFuzzyMatchInput_WithoutName.avro";
    private static final String WITHOUT_COUNTRY_FILENAME = "BulkFuzzyMatchInput_WithoutCountry.avro";
    private static final String WITHOUT_STATE_FILENAME = "BulkFuzzyMatchInput_WithoutState.avro";
    private static final String WITHOUT_CITY_FILENAME = "BulkFuzzyMatchInput_WithoutCity.avro";
    private static final String WITHOUT_STATECITY_FILENAME = "BulkFuzzyMatchInput_WithoutStateCity.avro";
    private static final String INCOMPLETELOCATION_FILENAME = "BulkFuzzyMatchInput_IncompleteLocation.avro";
    private static final String BULKMATCH_LOADTEST_FILENAME = "BulkFuzzyMatchInput_LoadTest.avro";
    private static final String NAME_PHONE_FILENAME = "BulkFuzzyMatchInput_NamePhone.avro";
    private static final String NAME_ZIPCODE_FILENAME = "BulkFuzzyMatchInput_NameZipCode.avro";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Test(groups = "deployment", dataProvider = "realtimeScenarios", enabled = false)
    public void testRealtimeMatchWithoutCache(String scenario) {
        MatchInput input = prepareRealtimeMatchInput(scenario, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        validateRealtimeMatchResult(scenario, output);
    }

    @Test(groups = "deployment", dataProvider = "realtimeScenarios", enabled = false)
    public void testRealtimeMatchWithCache(String scenario) {
        MatchInput input = prepareRealtimeMatchInput(scenario, true);
        MatchOutput output = matchProxy.matchRealTime(input);
        validateRealtimeMatchResult(scenario, output);
    }

    @DataProvider(name = "realtimeScenarios")
    private Iterator<Object[]> realtimeScenarios() {
        String[] scenarios = { SCENARIO_VALIDLOCATION, SCENARIO_VALIDLOCATION_INVALIDDOMAIN, SCENARIO_WITHOUT_NAME,
                SCENARIO_WITHOUT_COUNTRY, SCENARIO_WITHOUT_STATE, SCENARIO_WITHOUT_CITY, SCENARIO_WITHOUT_STATE_CITY,
                SCENARIO_INCOMPLETELOCATION, SCENARIO_NAME_PHONE, SCENARIO_NAME_ZIPCODE };
        List<Object[]> objs = new ArrayList<>();
        for (String scenario : scenarios) {
            objs.add(new Object[] { scenario });
        }
        return objs.iterator();
    }

    @Test(groups = "deployment", dataProvider = "bulkCatchScenarios", enabled = false)
    public void testBulkMatchWithCache(String scenario) {
        MatchInput input = prepareBulkMatchInput(scenario, true);

        MatchCommand finalStatus = runAndVerifyBulkMatch(input, podId);

        validateBulkMatchResult(scenario, finalStatus.getResultLocation());
    }

    @DataProvider(name = "bulkCatchScenarios")
    private Iterator<Object[]> bulkCatchScenarios() {
        String[] scenarios = {
                SCENARIO_VALIDLOCATION, //
                SCENARIO_VALIDLOCATION_INVALIDDOMAIN, //
                SCENARIO_WITHOUT_NAME, //
                SCENARIO_WITHOUT_COUNTRY, //
                SCENARIO_WITHOUT_STATE, //
                SCENARIO_WITHOUT_CITY, //
                SCENARIO_WITHOUT_STATE_CITY, //
                SCENARIO_INCOMPLETELOCATION, //
                SCENARIO_NAME_PHONE, //
                SCENARIO_NAME_ZIPCODE, //
                };
        List<Object[]> objs = new ArrayList<>();
        for (String scenario : scenarios) {
            objs.add(new Object[] { scenario });
        }
        return objs.iterator();
    }

    @Test(groups = "deployment", enabled = false)
    public void loadTestBulkMatchWithoutCache() {
        String[] scenarios = { SCENARIO_BULKMATCH_LOADTEST };
        for (String scenario : scenarios) {
            MatchInput input = prepareBulkMatchInput(scenario, false);
            MatchCommand command = matchProxy.matchBulk(input, podId);
            ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId);
            Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            MatchCommand matchCommand = matchCommandService.getByRootOperationUid(command.getRootOperationUid());
            Assert.assertEquals(matchCommand.getMatchStatus(), MatchStatus.FINISHED);

            MatchCommand finalStatus = matchProxy.bulkMatchStatus(command.getRootOperationUid());
            Assert.assertEquals(finalStatus.getApplicationId(), appId.toString());
            Assert.assertEquals(finalStatus.getRootOperationUid(), command.getRootOperationUid());
            Assert.assertEquals(finalStatus.getProgress(), 1f);
            Assert.assertEquals(finalStatus.getMatchStatus(), MatchStatus.FINISHED);
            Assert.assertEquals(finalStatus.getResultLocation(),
                    hdfsPathBuilder.constructMatchOutputDir(command.getRootOperationUid()).toString());
        }
    }

    /*
     * Test the DUNS redirect functionality. Without redirect, we should match to given src DUNS.
     * With redirect, we should get the target DUNS instead.
     */
    // FIXME re-enable this test after publication finishes
    @Test(groups = "deployment", enabled = false)
    public void testDunsGuideBookBulkMatch() throws Exception {
        Object[][] expectedResult = TestDunsGuideBookUtils.getDunsGuideBookTestData();
        String fullAvroDir = new Path(avroDir, DUNS_GUIDE_BOOK_DIR).toString();
        prepareDunsGuideBookData(fullAvroDir);
        AvroInputBuffer buf = new AvroInputBuffer();
        buf.setAvroDir(fullAvroDir);
        MatchInput input = TestDunsGuideBookUtils.newBulkMatchInput(DUNS_GUIDE_BOOK_DATACLOUD_VERSION,
                DECISION_GRAPH_WITHOUT_GUIDE_BOOK, true, true, buf, prepareColumnSelection());
        MatchCommand command = runAndVerifyBulkMatch(input, podId);
        Iterator<GenericRecord> records =
                AvroUtils.iterator(yarnConfiguration, command.getResultLocation() + "/*.avro");
        // should get srcDuns using the old decision graph
        validateMatchedDuns(records, expectedResult, 1); // expected source DUNS

        input = TestDunsGuideBookUtils.newBulkMatchInput(DUNS_GUIDE_BOOK_DATACLOUD_VERSION,
                DECISION_GRAPH_WITH_GUIDE_BOOK, true, true, buf, prepareColumnSelection());
        command = runAndVerifyBulkMatch(input, podId);
        records = AvroUtils.iterator(yarnConfiguration, command.getResultLocation() + "/*.avro");
        // redirect to the target DUNS
        validateMatchedDuns(records, expectedResult, 2); // expected target DUNS
    }

    private MatchInput prepareRealtimeMatchInput(String scenario, boolean useDnBCache) {
        MatchInput input = new MatchInput();
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        input.setDecisionGraph("DragonClaw");
        input.setLogLevelEnum(Level.DEBUG);
        input.setTenant(new Tenant("PD_Test"));
        input.setUseDnBCache(useDnBCache);
        input.setCustomSelection(prepareColumnSelection());
        input.setFields(prepareFields(scenario));
        input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        input.setData(prepareRealtimeData(scenario));
        input.setUseRemoteDnB(true);
        return input;
    }

    private ColumnSelection prepareColumnSelection() {
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = new ArrayList<Column>();
        for (String columnName : selectedColumns) {
            Column column = new Column(columnName, columnName);
            columns.add(column);
        }
        columnSelection.setColumns(columns);
        return columnSelection;
    }

    private List<String> prepareFields(String scenario) {
        List<String> fields = null;
        if (scenario != null && (scenario.equals(SCENARIO_VALIDLOCATION) || scenario.equals(SCENARIO_INCOMPLETELOCATION)
                || scenario.equals(SCENARIO_BULKMATCH_LOADTEST))) {
            fields = Arrays.asList("Name", "City", "State", "Country");
        } else if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION_INVALIDDOMAIN)) {
            fields = Arrays.asList("Name", "City", "State", "Country", "Domain");
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_NAME)) {
            fields = Arrays.asList("City", "State", "Country");
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_COUNTRY)) {
            fields = Arrays.asList("Name", "City", "State");
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE)) {
            fields = Arrays.asList("Name", "City", "Country");
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_CITY)) {
            fields = Arrays.asList("Name", "State", "Country");
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE_CITY)) {
            fields = Arrays.asList("Name", "Country");
        } else if (scenario != null && scenario.equals(SCENARIO_NAME_PHONE)) {
            fields = Arrays.asList("Name", "PhoneNumber");
        } else if (scenario != null && scenario.equals(SCENARIO_NAME_ZIPCODE)) {
            fields = Arrays.asList("Name", "Zipcode");
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
        return fields;
    }

    private List<List<Object>> prepareRealtimeData(String scenario) {
        List<List<Object>> data = new ArrayList<List<Object>>();
        if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[2]);
                d.add(loc[3]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION_INVALIDDOMAIN)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[2]);
                d.add(loc[3]);
                d.add(loc[4]);
                d.add(invalidDomain);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_NAME)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[2]);
                d.add(loc[3]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_COUNTRY)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[2]);
                d.add(loc[3]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[2]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_CITY)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[3]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE_CITY)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_INCOMPLETELOCATION)) {
            for (Object[] loc : nameLocationIncomplete) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[2]);
                d.add(loc[3]);
                d.add(loc[4]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_NAME_PHONE)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[5]);
                data.add(d);
            }
        } else if (scenario != null && scenario.equals(SCENARIO_NAME_ZIPCODE)) {
            for (Object[] loc : nameLocation) {
                List<Object> d = new ArrayList<Object>();
                d.add(loc[1]);
                d.add(loc[6]);
                data.add(d);
            }
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
        return data;
    }

    private void validateRealtimeMatchResult(String scenario, MatchOutput output) {
        log.info(String.format("Test scenario: %s", scenario));
        Assert.assertNotNull(output);

        Set<String> bothRowCanMatch = new HashSet<>(Arrays.asList(SCENARIO_VALIDLOCATION, //
                SCENARIO_VALIDLOCATION_INVALIDDOMAIN, //
                SCENARIO_WITHOUT_COUNTRY, //
                SCENARIO_WITHOUT_STATE, //
                SCENARIO_WITHOUT_CITY, //
                SCENARIO_WITHOUT_STATE_CITY, //
                SCENARIO_NAME_PHONE, //
                SCENARIO_NAME_ZIPCODE));

        if (bothRowCanMatch.contains(scenario)) {
            Assert.assertEquals(output.getResult().size(), 2);
            for (int i = 0; i < output.getResult().size(); i++) {
                OutputRecord outputRecord = output.getResult().get(i);
                Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
            }
        } else if (SCENARIO_WITHOUT_NAME.equals(scenario)) {
            Assert.assertEquals(output.getResult().size(), 2);
            for (int i = 0; i < output.getResult().size(); i++) {
                OutputRecord outputRecord = output.getResult().get(i);
                Assert.assertNull(outputRecord.getOutput().get(1));
            }
        } else if (SCENARIO_INCOMPLETELOCATION.equals(scenario)) {
            Assert.assertEquals(output.getResult().size(), 15);
            for (int i = 0; i < output.getResult().size(); i++) {
                OutputRecord outputRecord = output.getResult().get(i);
                Assert.assertEquals(outputRecord.getOutput().get(1), nameLocationIncomplete[i][0]);
            }
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
    }

    private MatchInput prepareBulkMatchInput(String scenario, boolean useDnBCache) {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setCustomSelection(prepareColumnSelection());
        input.setFields(prepareFields(scenario));
        input.setSkipKeyResolution(true);
        input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        input.setInputBuffer(prepareBulkData(scenario));
        input.setUseDnBCache(useDnBCache);
        input.setUseRemoteDnB(true);
        return input;
    }

    private InputBuffer prepareBulkData(String scenario) {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
        String fullAvroDir = new Path(avroDir, scenario).toString();
        cleanupAvroDir(fullAvroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        switch (scenario) {
            case SCENARIO_VALIDLOCATION:
                uploadTestAVro(fullAvroDir, VALIDLOCATION_FILENAME);
                break;
            case SCENARIO_VALIDLOCATION_INVALIDDOMAIN:
                uploadTestAVro(fullAvroDir, VALIDLOCATION_INVALIDDOMAIN_FILENAME);
                break;
            case SCENARIO_WITHOUT_NAME:
                uploadTestAVro(fullAvroDir, WITHOUT_NAME_FILENAME);
                break;
            case SCENARIO_WITHOUT_COUNTRY:
                uploadTestAVro(fullAvroDir, WITHOUT_COUNTRY_FILENAME);
                break;
            case SCENARIO_WITHOUT_STATE:
                uploadTestAVro(fullAvroDir, WITHOUT_STATE_FILENAME);
                break;
            case SCENARIO_WITHOUT_CITY:
                uploadTestAVro(fullAvroDir, WITHOUT_CITY_FILENAME);
                break;
            case SCENARIO_WITHOUT_STATE_CITY:
                uploadTestAVro(fullAvroDir, WITHOUT_STATECITY_FILENAME);
                break;
            case SCENARIO_INCOMPLETELOCATION:
                uploadTestAVro(fullAvroDir, INCOMPLETELOCATION_FILENAME);
                break;
            case SCENARIO_BULKMATCH_LOADTEST:
                uploadTestAVro(fullAvroDir, BULKMATCH_LOADTEST_FILENAME);
                break;
            case SCENARIO_NAME_PHONE:
                uploadTestAVro(fullAvroDir, NAME_PHONE_FILENAME);
                break;
            case SCENARIO_NAME_ZIPCODE:
                uploadTestAVro(fullAvroDir, NAME_ZIPCODE_FILENAME);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Scenario [%s] is not supported", scenario));
        }
        inputBuffer.setAvroDir(fullAvroDir);
        return inputBuffer;
    }

    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, String.format("matchinput/%s", fileName),
                    avroDir + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

    /*
     * clear hdfs directory for testing and upload test data as avro file
     */
    private void prepareDunsGuideBookData(String fullAvroDir) throws Exception {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
        cleanupAvroDir(fullAvroDir);
        AvroUtils.createAvroFileByData(yarnConfiguration, TestDunsGuideBookUtils.getBulkMatchAvroColumns(),
                TestDunsGuideBookUtils.getBulkMatchAvroData(), fullAvroDir, DUNS_GUIDE_BOOK_TEST_FILE);
    }


    /*
     * verify that matched DUNS in bulk match output matches the expected DUNS, dunsFieldIdx is used to specify
     * which index in the expected row is the DUNS.
     */
    private void validateMatchedDuns(Iterator<GenericRecord> records, Object[][] expectedData, int dunsFieldIdx) {
        Assert.assertNotNull(records);
        int count = 0;
        for (; records.hasNext(); count++) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record);
            Integer testId = (Integer) record.get(TestDunsGuideBookUtils.TEST_CASE_ID_FIELD);
            Assert.assertNotNull(testId);
            Object matchedDuns = record.get(DataCloudConstants.ATTR_LDC_DUNS);
            // use testId to retrieve the expected entry as the result can be out of order
            Object expectedDuns = expectedData[testId][dunsFieldIdx];
            if (expectedDuns == null) {
                Assert.assertNull(matchedDuns);
            } else {
                // matchedDuns can be Utf8 and not string
                Assert.assertEquals(matchedDuns.toString(), expectedDuns);
            }
        }
        Assert.assertEquals(count, expectedData.length);
    }

    private void validateBulkMatchResult(String scenario, String path) {
        log.info(String.format("Test scenario: %s", scenario));
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String ldcName = record.get("LDC_Name") == null ? null : record.get("LDC_Name").toString();
            String expectedName = record.get("ExpectedName") == null ? null : record.get("ExpectedName").toString();
            log.info(String.format("Expected name: %s; Actual name: %s", expectedName, ldcName));
            if (scenario != null && Arrays.asList(SCENARIO_VALIDLOCATION, SCENARIO_VALIDLOCATION_INVALIDDOMAIN,
                    SCENARIO_WITHOUT_COUNTRY, SCENARIO_WITHOUT_STATE, SCENARIO_WITHOUT_STATE_CITY, SCENARIO_NAME_PHONE,
                    SCENARIO_NAME_ZIPCODE).contains(scenario)) {
                Assert.assertEquals(ldcName, expectedName,
                        String.format("Expected name: %s; Actual name: %s", expectedName, ldcName));
            } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_NAME)) {
                Assert.assertNull(ldcName);
            } else if (SCENARIO_WITHOUT_CITY.equals(scenario) || SCENARIO_INCOMPLETELOCATION.equals(scenario)) {
                Assert.assertTrue(
                        (ldcName == null && expectedName == null)
                                || (expectedName != null && expectedName.equals(ldcName)),
                        String.format("Expected name: %s; Actual name: %s", expectedName, ldcName));
            } else {
                throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
            }
            rowNum++;
        }
        if (scenario.equals(SCENARIO_INCOMPLETELOCATION)) {
            Assert.assertEquals(rowNum, 15);
        } else {
            Assert.assertEquals(rowNum, 2);
        }
    }
}
