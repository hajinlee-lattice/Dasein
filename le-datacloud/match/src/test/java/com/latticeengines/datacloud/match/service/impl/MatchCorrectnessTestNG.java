package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;

@Component
public class MatchCorrectnessTestNG extends DataCloudMatchFunctionalTestNGBase {

    // up to 1000 input
    // domain, name, city, state, country +
    // expectedDomain, expectedName, expectedState, expectedCountry,
    // expectedEmp, expectedRev
    private static final Object[][] TEST_DATA = new Object[][] {
            // domain only easy cases
            { "google.com", null, null, null, null, "google.com", "Alphabet Inc.", "California", "USA", ">10,000", ">10B" },
            { "microsoft.com", null, null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },

            // name location only easy cases
            { null, "Alphabet Inc.", "Mountain View", "California", "USA", "abc.xyz", "Alphabet Inc.", "California", "USA", ">10,000", ">10B" },

            // short location, accurate spelling
            { null, "Alphabet", null, null, null, "abc.xyz", "Alphabet Inc.", "California", "USA", ">10,000", ">10B" },
            { null, "Google", null, null, null, "abc.xyz", "Google Inc.", "California", "USA", ">10,000", ">10B" },
            { null, "Microsoft", null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },

            // name standardization
            { null, "Johnson & Johnson", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },
            { null, "Johnson and Johnson", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },
            { null, "Johnson Johnson", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },

            { null, "Microsoft Corporation", null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            { null, "Microsoft Corp.", null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            { null, "Google Inc.", null, null, null, "abc.xyz", "Google Inc.", "California", "USA", ">10,000", ">10B" },

            { null, "Apple Inc", null, null, null, "apple.com", "Apple Inc.", "California", "USA", ">10,000", ">10B" },
            { null, "Apple", null, "CA", null, "apple.com", "Apple Inc.", "California", "USA", ">10,000", ">10B" },

            // oversea head quarter
            { null, "Royal Dutch Shell", null, null, "Netherlands", null, "ROYAL DUTCH SHELL plc", "ZUID-HOLLAND", "NETHERLANDS", ">10,000", "0-1M" },

            // us head quarter, oversea domestic ultimate
            { null, "Google UK", null, null, "UK", "google.co.uk", "GOOGLE UK LIMITED", "LONDON", "UNITED KINGDOM", "1001-2500", "1-5B" },

            //TODO: cases that should pass but cannot pass now
            // { "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA", "1", "1", "1", "1", "1", "1" }
            // { "apple.com", null, null, null, null, "1", "1", "1", "1", "1", "1" }
            // { null, "Micorsoft", null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
    };

    private static final int EXPECTED_DOMAIN_IDX = 5;
    private static final int EXPECTED_NAME_IDX = 6;
    private static final int EXPECTED_STATE_IDX = 7;
    private static final int EXPECTED_COUNTRY_IDX = 8;
    private static final int EXPECTED_EMP_IDX = 9;
    private static final int EXPECTED_REV_IDX = 10;

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Test(groups = "functional")
    public void testMatchCorrectness() {
        Object[][] data = addRowId(TEST_DATA);
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setPredefinedSelection(null);
        input.setCustomSelection(testMatchInputService.companyProfileSelection());
        input.setUseRemoteDnB(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        // expect every row to be matched
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(TEST_DATA.length));

        int[] idxMap = new int[] { //
                EXPECTED_DOMAIN_IDX, //
                EXPECTED_NAME_IDX, //
                EXPECTED_STATE_IDX, //
                EXPECTED_COUNTRY_IDX, //
                EXPECTED_EMP_IDX, //
                EXPECTED_REV_IDX };

        for (int i = 0; i < TEST_DATA.length; i++) {
            Assert.assertTrue(output.getResult().get(i).isMatched(), "This row is not matched: " + TEST_DATA[i]);
            List<Object> matchedRow = output.getResult().get(i).getOutput();
            for (int j = 0; j < idxMap.length; j++) {
                Assert.assertEquals(matchedRow.get(j), TEST_DATA[i][idxMap[j]]);
            }
        }
    }

    private Object[][] addRowId(Object[][] raw) {
        Object[][] toReturn = new Object[raw.length][6];
        for (int i = 0; i < raw.length; i++) {
            Object[] rawRow = raw[i];
            Object[] newRow = new Object[6];
            newRow[0] = i;
            for (int j = 0; j < 5; j++) {
                newRow[j + 1] = rawRow[j];
            }
            toReturn[i] = newRow;
        }
        return toReturn;
    }

}
