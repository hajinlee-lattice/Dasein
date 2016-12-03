package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
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

    private static final Log log = LogFactory.getLog(FuzzyMatchDeploymentTestNG.class);

    private static final String invalidDomain = "abcdefghijklmn.com";
    private static final String podId = "FuzzyMatchDeploymentTestNG";
    private static final String avroDir = "/tmp/FuzzyMatchDeploymentTestNG";

    // For realtime match: the first element is expected matched company name
    private static final Object[][] nameLocation = {
            { "Benchmark Blinds", "BENCHMARK BLINDS", "GILBERT", "ARIZONA", "United States" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", "America" } };

    // For realtime match: the first element is expected matched company name
    private static final Object[][] nameLocationIncomplete = { 
            { null, "", "MOUNTAIN VIEW", "CA", "America" },
            { null, null, "MOUNTAIN VIEW", "CA", "America" },
            { null, " +     * = # ", "MOUNTAIN VIEW", "CA", "America" },
            { "Google Inc.", "GOOGLE", "", "CA", "America" }, 
            { "Google Inc.", "GOOGLE", null, "CA", "America" },
            { "Google Inc.", "GOOGLE", " +     * = # ", "CA", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", null, "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", " +     * = # ", "America" },
            { "Google Inc.", "GOOGLE", "", "", "America" }, 
            { "Google Inc.", "GOOGLE", null, null, "America" },
            { "Google Inc.", "GOOGLE", " +     * = # ", " +     * = # ", "America" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", "" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", null },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", " +     * = # " } };

    private static final String[] selectedColumns = { "LatticeAccountId", "LDC_Name", "LDC_Domain", "LDC_Country",
            "LDC_State", "LDC_City", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "YEAR_STARTED" };

    private static final String SCENARIO_VALIDLOCATION = "ValidLocation";
    private static final String SCENARIO_VALIDLOCATION_INVALIDDOMAIN = "ValidLocationInvalidDomain";
    private static final String SCENARIO_WITHOUT_NAME = "WithoutName";
    private static final String SCENARIO_WITHOUT_COUNTRY = "WithoutCountry";
    private static final String SCENARIO_WITHOUT_STATE = "WithoutState";
    private static final String SCENARIO_WITHOUT_CITY = "WithoutCity";
    private static final String SCENARIO_WITHOUT_STATE_CITY = "WithoutStateCity";
    private static final String SCENARIO_INCOMPLETELOCATION = "IncompleteLocation";

    private static final String VALIDLOCATION_FILENAME = "BulkFuzzyMatchInput_ValidLocation.avro";
    private static final String VALIDLOCATION_INVALIDDOMAIN_FILENAME = "BulkFuzzyMatchInput_ValidLocation_InvalidDomain.avro";
    private static final String WITHOUT_NAME_FILENAME = "BulkFuzzyMatchInput_WithoutName.avro";
    private static final String WITHOUT_COUNTRY_FILENAME = "BulkFuzzyMatchInput_WithoutCountry.avro";
    private static final String WITHOUT_STATE_FILENAME = "BulkFuzzyMatchInput_WithoutState.avro";
    private static final String WITHOUT_CITY_FILENAME = "BulkFuzzyMatchInput_WithoutCity.avro";
    private static final String WITHOUT_STATECITY_FILENAME = "BulkFuzzyMatchInput_WithoutStateCity.avro";
    private static final String INCOMPLETELOCATION_FILENAME = "BulkFuzzyMatchInput_IncompleteLocation.avro";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MatchCommandService matchCommandService;

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatch() {
        String[] scenarios = { SCENARIO_VALIDLOCATION, SCENARIO_VALIDLOCATION_INVALIDDOMAIN, SCENARIO_WITHOUT_NAME,
                SCENARIO_WITHOUT_COUNTRY, SCENARIO_WITHOUT_STATE, SCENARIO_WITHOUT_CITY, SCENARIO_WITHOUT_STATE_CITY,
                SCENARIO_INCOMPLETELOCATION };
        for (String scenario : scenarios) {
            MatchInput input = prepareRealtimeMatchInput(scenario, false);
            MatchOutput output = matchProxy.matchRealTime(input);
            validateRealtimeMatchResult(scenario, output);

            input.setUseDnBCache(true);
            output = matchProxy.matchRealTime(input);
            validateRealtimeMatchResult(scenario, output);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testBulkMatch() {
        String[] scenarios = { SCENARIO_VALIDLOCATION, SCENARIO_VALIDLOCATION_INVALIDDOMAIN, SCENARIO_WITHOUT_NAME,
                SCENARIO_WITHOUT_COUNTRY, SCENARIO_WITHOUT_STATE, SCENARIO_WITHOUT_CITY, SCENARIO_INCOMPLETELOCATION };
        for (String scenario : scenarios) {
            MatchInput input = prepareBulkMatchInput(scenario, true);
            MatchCommand command = matchProxy.matchBulk(input, podId);
            ApplicationId appId = ConverterUtils.toApplicationId(command.getApplicationId());
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId);
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

            validateBulkMatchResult(scenario, finalStatus.getResultLocation());
        }

    }

    private MatchInput prepareRealtimeMatchInput(String scenario, boolean useDnBCache) {
        MatchInput input = new MatchInput();
        input.setDataCloudVersion("2.0.1");
        input.setDecisionGraph("DragonClaw");
        input.setLogLevel(Level.DEBUG);
        input.setTenant(new Tenant("PD_Test"));
        input.setUseDnBCache(useDnBCache);
        input.setCustomSelection(prepareColumnSelection());
        input.setFields(prepareFields(scenario));
        input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        input.setData(prepareRealtimeData(scenario));
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
        if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION)) {
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
        } else if (scenario != null && scenario.equals(SCENARIO_INCOMPLETELOCATION)) {
            fields = Arrays.asList("Name", "City", "State", "Country");
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
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
        return data;
    }

    private void validateRealtimeMatchResult(String scenario, MatchOutput output) {
        log.info(String.format("Test scenario: %s", scenario));
        Assert.assertNotNull(output);

        if (scenario != null
                && (scenario.equals(SCENARIO_VALIDLOCATION) || scenario.equals(SCENARIO_VALIDLOCATION_INVALIDDOMAIN))
                || scenario.equals(SCENARIO_WITHOUT_COUNTRY) || scenario.equals(SCENARIO_WITHOUT_STATE)
                || scenario.equals(SCENARIO_WITHOUT_STATE_CITY)) {
            Assert.assertEquals(output.getResult().size(), 2);
            for (int i = 0; i < output.getResult().size(); i++) {
                OutputRecord outputRecord = output.getResult().get(i);
                Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
            }
        } else if (scenario != null && (scenario.equals(SCENARIO_WITHOUT_NAME))) {
            Assert.assertEquals(output.getResult().size(), 2);
            for (int i = 0; i < output.getResult().size(); i++) {
                OutputRecord outputRecord = output.getResult().get(i);
                Assert.assertNull(outputRecord.getOutput().get(1));
            }
        } else if (scenario != null && (scenario.equals(SCENARIO_WITHOUT_CITY))) {
            Assert.assertEquals(output.getResult().size(), 2);
            Assert.assertNull(output.getResult().get(0).getOutput().get(1));
            Assert.assertEquals(output.getResult().get(1).getOutput().get(1), nameLocation[1][0]);
        } else if (scenario != null && (scenario.equals(SCENARIO_INCOMPLETELOCATION))) {
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
        input.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
        input.setExcludeUnmatchedWithPublicDomain(false);
        input.setPublicDomainAsNormalDomain(false);
        input.setFetchOnly(false);
        input.setSkipKeyResolution(true);
        input.setDecisionGraph("DragonClaw");
        input.setDataCloudVersion("2.0.1");
        input.setBulkOnly(false);
        input.setUseDnBCache(useDnBCache);
        input.setCustomSelection(prepareColumnSelection());
        input.setFields(prepareFields(scenario));
        input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        input.setInputBuffer(prepareBulkData(scenario));
        return input;
    }

    private InputBuffer prepareBulkData(String scenario) {
        HdfsPodContext.changeHdfsPodId(podId);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
        String fullAvroDir = new Path(avroDir, scenario).toString();
        cleanupAvroDir(fullAvroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION)) {
            uploadTestAVro(fullAvroDir, VALIDLOCATION_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_VALIDLOCATION_INVALIDDOMAIN)) {
            uploadTestAVro(fullAvroDir, VALIDLOCATION_INVALIDDOMAIN_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_NAME)) {
            uploadTestAVro(fullAvroDir, WITHOUT_NAME_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_COUNTRY)) {
            uploadTestAVro(fullAvroDir, WITHOUT_COUNTRY_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE)) {
            uploadTestAVro(fullAvroDir, WITHOUT_STATE_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_CITY)) {
            uploadTestAVro(fullAvroDir, WITHOUT_CITY_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_STATE_CITY)) {
            uploadTestAVro(fullAvroDir, WITHOUT_STATECITY_FILENAME);
        } else if (scenario != null && scenario.equals(SCENARIO_INCOMPLETELOCATION)) {
            uploadTestAVro(fullAvroDir, INCOMPLETELOCATION_FILENAME);
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
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

    private void validateBulkMatchResult(String scenario, String path) {
        log.info(String.format("Test scenario: %s", scenario));
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String ldcName = record.get("LDC_Name") == null ? null : record.get("LDC_Name").toString();
            String expectedName = record.get("ExpectedName") == null ? null : record.get("ExpectedName").toString();
            log.info(String.format("Expected name: %s; Actual name: %s", expectedName, ldcName));
            if (scenario != null
                    && (scenario.equals(SCENARIO_VALIDLOCATION) || scenario.equals(SCENARIO_VALIDLOCATION_INVALIDDOMAIN)
                            || scenario.equals(SCENARIO_WITHOUT_COUNTRY) || scenario.equals(SCENARIO_WITHOUT_STATE)
                            || scenario.equals(SCENARIO_WITHOUT_STATE_CITY))) {
                Assert.assertEquals(ldcName, expectedName);
            } else if (scenario != null && scenario.equals(SCENARIO_WITHOUT_NAME)) {
                Assert.assertNull(ldcName);
            } else if (scenario != null
                    && (scenario.equals(SCENARIO_WITHOUT_CITY) || scenario.equals(SCENARIO_INCOMPLETELOCATION))) {
                Assert.assertTrue((ldcName == null && expectedName == null) || expectedName.equals(ldcName));
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
