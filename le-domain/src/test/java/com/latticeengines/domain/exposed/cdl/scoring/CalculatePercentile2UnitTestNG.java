package com.latticeengines.domain.exposed.cdl.scoring;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import au.com.bytecode.opencsv.CSVReader;

public class CalculatePercentile2UnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed" //
            + "/cdl/scoring/CalculatePercentile2UnitTestNG/";
    private static final String FAKE_MODEL_ID = "Fake_Model_Id";

    private final Map<String, String> targetScoreDerivation = new HashMap<>();
    private CalculatePercentile2 calculatorWithTargetScoreDerivation;

    private final Map<String, String> scoreDerivation = new HashMap<>();
    private CalculatePercentile2 calculatorWithScoreDerivation;

    @BeforeClass(groups = "unit")
    public void setup() {
        try {
            String jsonFile = RESOURCE_ROOT + "targetscorederivation.json";
            targetScoreDerivation.put(FAKE_MODEL_ID,
                    IOUtils.toString(
                            Thread.currentThread().getContextClassLoader().getResourceAsStream(jsonFile),
                            StandardCharsets.UTF_8));
            calculatorWithTargetScoreDerivation = new CalculatePercentile2(5, 99, true, targetScoreDerivation);

            String jsonFile2 = RESOURCE_ROOT + "scorederivation.json";
            scoreDerivation.put(FAKE_MODEL_ID,
                    IOUtils.toString(
                            Thread.currentThread().getContextClassLoader().getResourceAsStream(jsonFile2),
                            StandardCharsets.UTF_8));
            calculatorWithScoreDerivation = new CalculatePercentile2(5, 99, true, scoreDerivation);

        } catch (IOException e) {
            throw new AssertionError("Failed read targetscorederivation from json file.", e);
        }
    }

    @Test(groups = "unit", dataProvider = "TestSet")
    public void testPercentileScore(double rawScore, int scoreWTargetScoreDerivation, int scoreWScoreDerivation) {
        int score1 = calculatorWithTargetScoreDerivation.calculate(FAKE_MODEL_ID, rawScore, 100, 0);
        Assert.assertEquals(score1, scoreWTargetScoreDerivation);

        int score2= calculatorWithScoreDerivation.calculate(FAKE_MODEL_ID, rawScore, 100, 0);
        Assert.assertEquals(score2, scoreWScoreDerivation);
    }

    @DataProvider(name = "TestSet")
    private Iterator<Object[]> peacePeriodTestData() {
        ArrayList<Object[]> dataset = new ArrayList<>();
        String csvFile = RESOURCE_ROOT + "testset.csv";
        InputStreamReader reader = new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(csvFile));
        try (CSVReader csvReader = new CSVReader(reader)) {
            csvReader.readNext(); // skip over header row
            String[] rows = csvReader.readNext();
            while (rows != null) {
                dataset.add(new Object[] {Double.parseDouble(rows[0]), Integer.parseInt(rows[1]), Integer.parseInt(rows[2])});
                rows = csvReader.readNext();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataset.stream().iterator();
    }
}
