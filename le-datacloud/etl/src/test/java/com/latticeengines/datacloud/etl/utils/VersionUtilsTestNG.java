package com.latticeengines.datacloud.etl.utils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

public class VersionUtilsTestNG {

    private static final Calendar CALENDAR = prepareCalendar();
    private static final List<String> BOMBORA_FILES = prepareBomboraFiles();
    private static final List<String> DNB_FILES = prepareDnBFiles();
    private static final String BOMBORA_TS = "yyyyMMdd";
    private static final String DNB_TS = "yyyy_MM";

    @Test(groups = "unit", dataProvider = "versionedPaths")
    private void testGetMostRecentVersionPaths(List<String> inputPaths, VersionCheckStrategy checkStrategy,
            String tsPattern, Integer nPeriod, List<String> expectedOutput) {
        List<String> outputPaths = VersionUtils.getMostRecentVersionPaths(inputPaths, nPeriod, checkStrategy, tsPattern,
                CALENDAR);
        Collections.sort(outputPaths);
        Collections.sort(expectedOutput);
        Assert.assertArrayEquals(
                String.format(
                        "VersionCheckStrategy: %s, nPeriod: %d, Input paths: %s; expected output paths: %s; actual output paths: %s",
                        checkStrategy, nPeriod, String.join(",", inputPaths), String.join(",", expectedOutput),
                        String.join(",", outputPaths)),
                expectedOutput.toArray(new String[0]), outputPaths.toArray(new String[0]));
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
}
