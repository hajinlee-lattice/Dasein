package com.latticeengines.datacloud.etl.utils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

public class VersionUtilsTestNG {

    private static final Calendar CALENDAR = prepareCalendar();
    private static final List<String> BOMBORA_FILES = prepareBomboraFiles();
    private static final List<String> DNB_FILES = prepareDnBFiles();
    private static final String BOMBORA_TS = "yyyyMMdd";
    private static final String DNB_TS = "yyyy_MM";

    @Test(groups = "unit", dataProvider = "versionedPaths")
    public void testGetMostRecentVersionPaths(List<String> inputPaths, VersionCheckStrategy checkStrategy,
            String tsPattern, Integer nPeriod, List<String> expectedOutput) {
        List<String> outputPaths = VersionUtils.getMostRecentVersionPaths(inputPaths, nPeriod, checkStrategy, tsPattern,
                null, CALENDAR);
        Collections.sort(outputPaths);
        Collections.sort(expectedOutput);
        Assert.assertEquals(
                expectedOutput.toArray(new String[0]), outputPaths.toArray(new String[0]),
                String.format(
                        "VersionCheckStrategy: %s, nPeriod: %d, Input paths: %s; expected output paths: %s; actual output paths: %s",
                        checkStrategy, nPeriod, String.join(",", inputPaths), String.join(",", expectedOutput),
                        String.join(",", outputPaths)));
    }

    @Test(groups = "unit", dataProvider = "tsVersions")
    public void testExtractTSVersion(String input, String output, String originTSPattern, String tsRegex,
            String targetTSPattern, String timeZone, boolean isValidTSPattern) {
        if (isValidTSPattern) {
            Assert.assertEquals(
                    VersionUtils.extractTSVersion(input, originTSPattern, tsRegex, targetTSPattern, timeZone),
                    output);
        } else {
            Assert.assertThrows(Exception.class,
                    () -> VersionUtils.extractTSVersion(input, originTSPattern, tsRegex, targetTSPattern, timeZone));
        }
    }

    @DataProvider(name = "versionedPaths")
    private Object[][] getVersionedPaths() {
        return new Object[][] {
                { BOMBORA_FILES, VersionCheckStrategy.ALL, BOMBORA_TS, null, BOMBORA_FILES }, //
                { BOMBORA_FILES, VersionCheckStrategy.DAY, BOMBORA_TS, 0, Collections.emptyList() }, //
                { BOMBORA_FILES, VersionCheckStrategy.DAY, BOMBORA_TS, 1,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.DAY, BOMBORA_TS, 8,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.DAY, BOMBORA_TS, 9,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz", "Bombora_Firehose_20191222.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.WEEK, BOMBORA_TS, 0,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.WEEK, BOMBORA_TS, 1,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz", "Bombora_Firehose_20191222.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.MONTH, BOMBORA_TS, 0,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz", "Bombora_Firehose_20191222.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.MONTH, BOMBORA_TS, 1,
                        Arrays.asList("Bombora_Firehose_20191230.csv.gz", "Bombora_Firehose_20191222.csv.gz",
                                "Bombora_Firehose_20191101.csv.gz") }, //
                { BOMBORA_FILES, VersionCheckStrategy.MONTH, BOMBORA_TS, 2, BOMBORA_FILES }, //

                { DNB_FILES, VersionCheckStrategy.ALL, DNB_TS, null, DNB_FILES }, //
                { DNB_FILES, VersionCheckStrategy.MONTH, DNB_TS, 0,
                        Arrays.asList("LE_SEED_OUTPUT_2019_12_TEST1.OUT.gz", "LE_SEED_OUTPUT_2019_12_TEST2.OUT.gz") }, //
                { DNB_FILES, VersionCheckStrategy.MONTH, DNB_TS, 1, Arrays.asList("LE_SEED_OUTPUT_2019_12_TEST1.OUT.gz",
                        "LE_SEED_OUTPUT_2019_12_TEST2.OUT.gz", "LE_SEED_OUTPUT_2019_11_TEST1.OUT.gz") }, //
                { DNB_FILES, VersionCheckStrategy.MONTH, DNB_TS, 11, DNB_FILES }, //
        };
    }

    private static Calendar prepareCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.AM_PM, Calendar.AM);
        calendar.set(Calendar.MONTH, Calendar.DECEMBER);
        calendar.set(Calendar.DAY_OF_MONTH, 31);
        calendar.set(Calendar.YEAR, 2019);
        return calendar;
    }
    
    private static List<String> prepareBomboraFiles() {
        return Arrays.asList( //
                "Bombora_Firehose_20191230.csv.gz", //
                "Bombora_Firehose_20191222.csv.gz", //
                "Bombora_Firehose_20191101.csv.gz", //
                "Bombora_Firehose_20191001.csv.gz" //
                );
    }

    private static List<String> prepareDnBFiles() {
        return Arrays.asList(//
                "LE_SEED_OUTPUT_2019_12_TEST1.OUT.gz", //
                "LE_SEED_OUTPUT_2019_12_TEST2.OUT.gz", //
                "LE_SEED_OUTPUT_2019_11_TEST1.OUT.gz", //
                "LE_SEED_OUTPUT_2019_01_TEST1.OUT.gz"
        );
    }

    // Schema: input, output, originTSPattern, tsRegex, targetTSPattern,
    // timeZone, isValidTSPattern
    @DataProvider(name = "tsVersions")
    private Object[][] getTSVersions() {
        return new Object[][] { //
                { "2000ABC01", null, "yyyyMM", null, null, null, true }, //
                { "ABC20000901", "200009", "yyyyMM", null, null, null, true }, //
                { "ABC20000901", "2000-09", "yyyyMM", null, "yyyy-MM", null, true }, //
                { "A2019-01-01A", "01/01/2019", "yyyy-MM-dd", null, "MM/dd/yyyy", null, true }, //
                { "2019_01", "01/01/2019", "yyyy_MM", null, "MM/dd/yyyy", null, true }, //
                { "20190101", "2019-01-01_00-00-00_UTC", "yyyyMMdd", null,
                        HdfsPathBuilder.DATE_FORMAT_STRING, HdfsPathBuilder.UTC, true }, //
                { "2019-01-01_00-00-00_UTC", "20190101", HdfsPathBuilder.DATE_FORMAT_STRING, "(.+)", "yyyyMMdd",
                        HdfsPathBuilder.UTC, true }, //

                { "2019-01-01", null, "abc", null, "MM/dd/yy", null, false }, //
                { "2019-01-01", null, "yyyy-MM-dd", null, "abc", null, false }, //
        };
    }
}
