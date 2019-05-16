package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Component
public class MatchCorrectnessTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String[] inputFields = new String[] { "Id", "Domain", "Name", "City", "State", "ZipCode", "Country", "PhoneNumber" };

    // domain,
    // name,
    // city,
    // state,
    // zipcode,
    // country,
    // phonenumber
    // expectedDomain,
    // expectedName,
    // expectedState,
    // expectedCountry,
    // expectedEmp,
    // expectedRev

    // domain only easy cases
    private static final Object[][] TEST_DATA_1 = new Object[][] { //
            { "google.com", null, null, null, null, null, null, "google.com", "GOOGLE LLC", "California", "USA",
                    ">10,000", ">10B" },
            { "microsoft.com", null, null, null, null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            { "apple.com", null, null, null, null, null, null, "apple.com", "Apple Inc.", "California", "USA", ">10,000", ">10B" },
            { "chevron.com", null, null, null, null, null, null, "chevron.com", "Chevron Corporation", "California", "USA", ">10,000", ">10B" },
            // ge.com, bk.com
    };

    // name location only easy cases
    private static final Object[][] TEST_DATA_2 = new Object[][] { //
            { null, "Alphabet Inc.", "Mountain View", "California", null, "USA", null, "ABC.XYZ", "Alphabet Inc.",
                    "California", "USA", ">10,000", ">10B" },
            { null, "Chevron Corporation", "San Ramon", "California", null, "USA", null, "chevron.com", "Chevron Corporation", "California", "USA", ">10,000", ">10B" },
    };

    // short location, accurate spelling
    private static final Object[][] TEST_DATA_3 = new Object[][] { //
            { null, "Alphabet", null, null, null, null, null, "ABC.XYZ", "Alphabet Inc.", "California", "USA",
                    ">10,000", ">10B" },
            /*{ null, "Google", null, null, null, null, null, "google.com", "GOOGLE LLC", "California", "USA", ">10,000",
                    ">10B" },*/
            { null, "Microsoft", null, null, null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
    };

    // name standardization
    private static final Object[][] TEST_DATA_4 = new Object[][] { //            
            { null, "Johnson & Johnson", null, "NJ", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey",
                    "USA", ">10,000", ">10B" },
            { null, "Johnson and Johnson", null, "NJ", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey",
                    "USA", ">10,000", ">10B" },
            { null, "Johnson Johnson", null, "NJ", null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey",
                    "USA", ">10,000", ">10B" },
            { null, "Johnson & Johnson", null, null, null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },
            { null, "Johnson and Johnson", null, null, null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },
            { null, "Johnson Johnson", null, null, null, null, null, "jnj.com", "Johnson & Johnson", "New Jersey", "USA", ">10,000", ">10B" },

            { null, "Microsoft Corporation", null, null, null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            { null, "Microsoft Corp.", null, null, null, null, null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            { null, "Google Inc.", null, null, null, null, null, "abc.xyz", "ALPHABET INC.",
                    "California", "USA", ">10,000", ">10B" },
            // { null, "Apple Inc", null, null, null, "apple.com", "Apple Inc.", "California", "USA", ">10,000", ">10B" },
            // { null, "Apple", null, "CA", null, "apple.com", "Apple Inc.", "California", "USA", ">10,000", ">10B" },
    };

    // oversea head quarter
    private static final Object[][] TEST_DATA_5 = new Object[][] { //            
            // { null, "Royal Dutch Shell", null, null, null, "Netherlands", null, "shell.com", "ROYAL DUTCH SHELL plc","ZUID HOLLAND", "NETHERLANDS", ">10,000", ">10B" },
    };

    // us head quarter, oversea domestic ultimate
    private static final Object[][] TEST_DATA_6 = new Object[][] { //            
            // { null, "Google UK", null, null, null, "UK", null, "google.co.uk", "GOOGLE UK LIMITED", "LONDON", "UNITED KINGDOM", "1001-2500", "1-5B" },
    };

    // slight mis-spell in name
    private static final Object[][] TEST_DATA_7 = new Object[][] { //            
            //{ null, "Microsoft Corp1", null, "WA", null, "USA", null, "microsoft.com", "Microsoft Corporation", "Washington", "USA", ">10,000", ">10B" },
            //{ null, "Alphabet Inc1", null, "California", null, "US", null, "google.com", "Alphabet Inc.", "California", "USA", ">10,000", ">10B" },
            { null, "Eversource Energy", "Springfield", "MA", "273608923", "US", "3368895000", "eversource.com",
                    "EVERSOURCE ENERGY", "MASSACHUSETTS", "USA", "5001-10,000", "5B-10B" },
            //{ null, "Queens College", "Queens", "NY", "11367-1597", "US", "7189975000", "queensknights.com", "Department of Media Studies", "New York", "USA", "11-50", "0-1M" },
            //{ null, "Edison47", "Port Orchard", "WA", "98367", "US", "3608746772", "edison47.com",  "Edison", "Washington", "USA", "1-10", "0-1M" },
            // { null, "SS&C Advent", "San Francisco", "CA", "950142083", "US", "4089961010", "edison47.com",  "Edison", "Washington", "USA", "1-10", "0-1M" },
    };

    private static final int EXPECTED_DOMAIN_IDX = 7;
    private static final int EXPECTED_NAME_IDX = 8;
    private static final int EXPECTED_STATE_IDX = 9;
    private static final int EXPECTED_COUNTRY_IDX = 10;
    private static final int EXPECTED_EMP_IDX = 11;
    private static final int EXPECTED_REV_IDX = 12;

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Test(groups = "functional", dataProvider = "TestData")
    public void testMatchCorrectness(Object[] row) {
        Object[][] data = addRowId(new Object[][]{ row });
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data, inputFields);
        input.setPredefinedSelection(null);
        input.setCustomSelection(testMatchInputService.companyProfileSelection());
        input.setUseRemoteDnB(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);

        int[] idxMap = new int[] { //
                EXPECTED_DOMAIN_IDX, //
                EXPECTED_NAME_IDX, //
                EXPECTED_STATE_IDX, //
                EXPECTED_COUNTRY_IDX, //
                EXPECTED_EMP_IDX, //
                EXPECTED_REV_IDX };

        OutputRecord record = output.getResult().get(0);
        Assert.assertTrue(record.isMatched(), "This row is not matched: " + StringUtils.join(row, ","));
        List<Object> matchedRow = record.getOutput();
        for (int j = 0; j < idxMap.length; j++) {
            Assert.assertEquals(String.valueOf(matchedRow.get(j)).toUpperCase(),
                    String.valueOf(row[idxMap[j]]).toUpperCase(), "Testing Data: " + StringUtils.join(row, ","));
        }
    }

    @DataProvider(name = "TestData")
    private Object[][] testData() {
        Object[][] data = ArrayUtils.addAll(TEST_DATA_1, TEST_DATA_2);
        data = ArrayUtils.addAll(data, TEST_DATA_3);
        data = ArrayUtils.addAll(data, TEST_DATA_4);
        data = ArrayUtils.addAll(data, TEST_DATA_5);
        data = ArrayUtils.addAll(data, TEST_DATA_6);
        data = ArrayUtils.addAll(data, TEST_DATA_7);
        return data;
    }

    private Object[][] addRowId(Object[][] raw) {
        Object[][] toReturn = new Object[raw.length][8];
        for (int i = 0; i < raw.length; i++) {
            Object[] rawRow = raw[i];
            Object[] newRow = new Object[8];
            newRow[0] = i;
            System.arraycopy(rawRow, 0, newRow, 1, 7);
            toReturn[i] = newRow;
        }
        return toReturn;
    }

}
