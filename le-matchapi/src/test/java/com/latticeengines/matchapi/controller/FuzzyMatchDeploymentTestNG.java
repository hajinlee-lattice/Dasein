package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

@Component
public class FuzzyMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final String invalidDomain = "abcdefghijklmn.com";

    // the first element is expected matched company name
    private static final Object[][] nameLocation = {
            { "Benchmark Blinds", "BENCHMARK BLINDS", "GILBERT", "ARIZONA", "United States" },
            { "Google Inc.", "GOOGLE", "MOUNTAIN VIEW", "CA", "America" } };

    private static final String[] selectedColumns = { "LatticeAccountId", "LDC_Name", "LDC_Domain", "LDC_Country",
            "LDC_State", "LDC_City", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "YEAR_STARTED" };

    private static final String SCENARIO_VALIDLOCATION = "ValidLocation";
    private static final String SCENARIO_VALIDLOCATION_INVALIDDOMAIN = "ValidLocationInvalidDomain";
    private static final String SCENARIO_WITHOUT_NAME = "WithoutName";
    private static final String SCENARIO_WITHOUT_COUNTRY = "WithoutCountry";
    private static final String SCENARIO_WITHOUT_STATE = "WithoutState";
    private static final String SCENARIO_WITHOUT_CITY = "WithoutCity";

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchValidLocation() {
        MatchInput input = prepareMatchInput(SCENARIO_VALIDLOCATION, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchValidLocationInvalidDomain() {
        MatchInput input = prepareMatchInput(SCENARIO_VALIDLOCATION_INVALIDDOMAIN, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchWithoutName() {
        MatchInput input = prepareMatchInput(SCENARIO_WITHOUT_NAME, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertNull(outputRecord.getOutput().get(1));
        }

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertNull(outputRecord.getOutput().get(1));
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchWithoutCountry() {
        MatchInput input = prepareMatchInput(SCENARIO_WITHOUT_COUNTRY, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchWithoutState() {
        MatchInput input = prepareMatchInput(SCENARIO_WITHOUT_STATE, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        for (int i = 0; i < output.getResult().size(); i++) {
            OutputRecord outputRecord = output.getResult().get(i);
            Assert.assertEquals(outputRecord.getOutput().get(1), nameLocation[i][0]);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testRealtimeMatchWithoutCity() {
        MatchInput input = prepareMatchInput(SCENARIO_WITHOUT_CITY, false);
        MatchOutput output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertNull(output.getResult().get(0).getOutput().get(1));
        Assert.assertEquals(output.getResult().get(1).getOutput().get(1), nameLocation[1][0]);

        input.setUseDnBCache(true);
        output = matchProxy.matchRealTime(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertNull(output.getResult().get(0).getOutput().get(1));
        Assert.assertEquals(output.getResult().get(1).getOutput().get(1), nameLocation[1][0]);
    }

    private MatchInput prepareMatchInput(String scenario, boolean useDnBCache) {
        MatchInput input = new MatchInput();
        input.setDataCloudVersion("2.0.0");
        input.setDecisionGraph("DragonClaw");
        input.setLogLevel(Level.DEBUG);
        input.setTenant(new Tenant("PD_Test"));
        input.setUseDnBCache(useDnBCache);
        input.setCustomSelection(prepareColumnSelection());
        input.setFields(prepareFields(scenario));
        input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        input.setData(prepareData(scenario));
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
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
        return fields;
    }

    private List<List<Object>> prepareData(String scenario) {
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
        } else {
            throw new UnsupportedOperationException(String.format("%s scenario is not supported", scenario));
        }
        return data;
    }

}
