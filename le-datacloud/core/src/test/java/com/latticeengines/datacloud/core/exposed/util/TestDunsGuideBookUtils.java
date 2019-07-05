package com.latticeengines.datacloud.core.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

public class TestDunsGuideBookUtils {
    public static final String TEST_CASE_ID_FIELD = "TestId"; // field used to identify test cases

    /**
     * Generate {@link MatchKeyTuple} from commonly used name location fields
     */
    public static MatchKeyTuple newTuple(String name, String country, String state, String city) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setName(name);
        tuple.setCountry(country);
        tuple.setState(state);
        tuple.setCity(city);
        return tuple;
    }

    /**
     * Generate {@link MatchInput} with given {@link MatchKeyTuple} as data and use the specified DataCloud version
     * and decision graph for real time match
     * @param dataCloudVersion version used in the matcher
     * @param decisionGraph decision graph name used in the matcher
     * @param tuple contains match input data for each {@link MatchKey}
     * @return generated match input that can be used for real time match
     */
    public static MatchInput newRealtimeMatchInput(
            @NotNull String dataCloudVersion, @NotNull String decisionGraph, @NotNull MatchKeyTuple tuple) {
        MatchInput input = getBaseMatchInput(dataCloudVersion, decisionGraph);
        input.setPredefinedSelection(ColumnSelection.Predefined.CompanyProfile);

        setMatchKeys(input, tuple);
        setValues(input, tuple);

        // get match logs
        input.setLogLevelEnum(Level.DEBUG);

        return input;
    }

    /**
     * Generate {@link MatchInput} with given {@link MatchKeyTuple} as data and use the specified DataCloud version
     * and decision graph for bulk match
     * @param dataCloudVersion version used in the matcher
     * @param decisionGraph decision graph name used in the matcher
     * @param useDnBCache flag to use DnB cache for match
     * @param useRemoteDnB flag to use DnB API for match
     * @param inputBuffer contains the location of bulk match input file
     * @param columnSelection specify which columns are required in output
     * @return generated match input that can be used for bulk match
     */
    public static MatchInput newBulkMatchInput(
            @NotNull String dataCloudVersion, @NotNull String decisionGraph, boolean useDnBCache,
            boolean useRemoteDnB, @NotNull InputBuffer inputBuffer, @NotNull ColumnSelection columnSelection) {
        MatchInput input = getBaseMatchInput(dataCloudVersion, decisionGraph);
        // predefined name location columns
        MatchKeyTuple tuple = newTuple(MatchKey.Name.name(), MatchKey.Country.name(),
                MatchKey.State.name(), MatchKey.City.name());
        setMatchKeys(input, tuple);
        input.setInputBuffer(inputBuffer);
        input.setUseDnBCache(useDnBCache);
        input.setUseRemoteDnB(useRemoteDnB);
        input.setCustomSelection(columnSelection);
        return input;
    }

    /**
     * Generate a list of columns for bulk match's input avro file
     *
     * Must be in sync with {@link TestDunsGuideBookUtils#getBulkMatchAvroData()}
     * @return a list of Pair(FIELD NAME, COLUMN TYPE)
     */
    public static List<Pair<String, Class<?>>> getBulkMatchAvroColumns() {
        return Arrays.asList(
                Pair.of(TEST_CASE_ID_FIELD, Integer.class), // test case ID
                Pair.of(MatchKey.Name.name(), String.class),
                Pair.of(MatchKey.Country.name(), String.class),
                Pair.of(MatchKey.State.name(), String.class),
                Pair.of(MatchKey.City.name(), String.class)
        );
    }

    /**
     * Generate test data for bulk match tests
     *
     * Must be in sync with {@link TestDunsGuideBookUtils#getBulkMatchAvroColumns()}
     * @return a two dimensional array where the first dimension represent test cases and the second dimension
     * represent the columns in each test case.
     */
    public static Object[][] getBulkMatchAvroData() {
        Object[][] testData = getDunsGuideBookTestData();
        Object[][] bulkMatchAvro = new Object[testData.length][];
        for (int i = 0; i < testData.length; i++) {
            MatchKeyTuple tuple = (MatchKeyTuple) testData[i][0];
            bulkMatchAvro[i] = new Object[] {
                    i, tuple.getName(), tuple.getCountry(), tuple.getState(), tuple.getCity() };
        }
        return bulkMatchAvro;
    }

    /**
     * Return test cases for redirect functionality provided by DunsGuideBook. Each test cases is an array that contains
     * the following columns:
     * 1. matchKeyTuple: use to provide name location values
     * 2. expectedSourceDuns: expected matched DUNS without redirect (can be null)
     * 3. expectedTargetDuns: expected matched DUNS with redirect (can be null)
     * 4. expectedBookSource: expected book source used for redirection (can be null)
     *
     * @return a two dimensional array where the first dimension represent test cases and the second dimension
     * represent the columns in each test case.
     */
    public static Object[][] getDunsGuideBookTestData() {
        return new Object[][] { //

                /* ManualMatch redirect book */
                { //
                        newTuple("Lane Gin", null, null, null), //
                        "602759081", "859029956", //
                        "ManualMatch" //
                }, //
                { //
                        newTuple("Body Central", "USA", null, null), //
                        "052267488", "151962479", //
                        "ManualMatch" //
                }, //
                { //
                        newTuple("Valmont Newmark, Inc.", "USA", "NEBRASKA", null), //
                        "828616990", "007267214", //
                        "ManualMatch" //
                }, //
                { //
                        newTuple("G.I.A. Publications Inc", "USA", null, "Chicago"), //
                        "080935386", "062501705", //
                        "ManualMatch" //
                }, //

                /* Domain DUNS Map redirect book */
                // { newTuple("H&R Block Enterprises LLC", "USA", "Texas", null),
                // "018628337", "167155829", "DomDunsMap" },
                // { newTuple("H&R Block Enterprises LLC", "USA", null, null),
                // "005491814", "043951235", "DomDunsMap" },
                // { newTuple("H&R Block Enterprises LLC", null, null, null),
                // "005491814", "043951235", "DomDunsMap" },

                /* DUNS Tree redirect book */
                { //
                        newTuple("Bio-Medical Applications of California, Inc.", null, null, null), //
                        "831572776", "324661834", //
                        "DunsTree" //
                },

                /* No redirect (no target DUNS for given KeyPartition) */

                // empty DunsGuideBook.Items
                { //
                        newTuple("Vox Mobile", null, null, null), //
                        "805888638", "805888638", null //
                }, //
                { //
                        newTuple("Vox Mobile", "USA", null, null), //
                        "805888638", "805888638", null //
                }, //
                { //
                        newTuple("Vox Mobile", "USA", "Ohio", null), //
                        "805888638", "805888638", null //
                }, //
                { //
                        newTuple("Vox Mobile", "USA", null, "Independence"), //
                        "805888638", "805888638", null //
                }, //

                // only one entry (KeyPartition=Name) in DunsGuideBook.Items
                { //
                        newTuple("Posco", "USA", null, null), //
                        "156920258", "156920258", null //
                }, //
                { //
                        newTuple("Posco", "USA", "Georgia", null), //
                        "156920258", "156920258", null //
                }, //
                { //
                        newTuple("Posco", "USA", null, "Johns Creek"), //
                        "156920258", "156920258", null //
                }, //
                { //
                        newTuple("Posco", "USA", "Georgia", "Johns Creek"), //
                        "156920258", "156920258", null //
                }, //

                // DUNS does not exist in AM (should get 010418671 in traveler log, just in
                // case)
                { //
                        newTuple("South Florida Insulation Co", "USA", null, null), //
                        null, null, null //
                }, //
        }; //
    }

    private static MatchInput getBaseMatchInput(@NotNull String dataCloudVersion, @NotNull String decisionGraph) {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        input.setDataCloudVersion(dataCloudVersion);
        input.setDecisionGraph(decisionGraph);
        return input;
    }

    /*
     * set match field name and key map (must be in sync with setMatchKeys)
     */
    private static void setMatchKeys(@NotNull MatchInput input, @NotNull MatchKeyTuple tuple) {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        List<String> fields = new ArrayList<>();
        if (tuple.getName() != null) {
            addMatchKey(keyMap, fields, MatchKey.Name);
        }
        if (tuple.getCountry() != null) {
            addMatchKey(keyMap, fields, MatchKey.Country);
        }
        if (tuple.getState() != null) {
            addMatchKey(keyMap, fields, MatchKey.State);
        }
        if (tuple.getCity() != null) {
            addMatchKey(keyMap, fields, MatchKey.City);
        }
        input.setFields(fields);
        input.setKeyMap(keyMap);
    }

    /*
     * set match data for real time match (must be in sync with setMatchKeys)
     */
    private static void setValues(@NotNull MatchInput input, @NotNull MatchKeyTuple tuple) {
        List<Object> values = new ArrayList<>();
        if (tuple.getName() != null) {
            values.add(tuple.getName());
        }
        if (tuple.getCountry() != null) {
            values.add(tuple.getCountry());
        }
        if (tuple.getState() != null) {
            values.add(tuple.getState());
        }
        if (tuple.getCity() != null) {
            values.add(tuple.getCity());
        }
        input.setData(Collections.singletonList(values));
    }

    private static void addMatchKey(
            @NotNull Map<MatchKey, List<String>> keyMap, @NotNull List<String> fields, @NotNull MatchKey matchKey) {
        fields.add(matchKey.name());
        keyMap.put(matchKey, Collections.singletonList(matchKey.name()));
    }
}
