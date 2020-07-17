package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToCsvTransformer;

public class RecommendationAvroToCsvTransformerTestNG {
    private static final Logger log = LoggerFactory.getLogger(RecommendationAvroToCsvTransformerTestNG.class);

    private Map<String, String> accountDisplayNames;
    private Map<String, String> contactDisplayNames;

    @BeforeClass(groups = "unit")
    public void setup() throws IOException {
        accountDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/account_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(accountDisplayNames));
        contactDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/contact_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(contactDisplayNames));
    }

    @Test(groups = "unit")
    public void testS3ExportFields() {
        RecommendationAvroToCsvTransformer transformer = new RecommendationAvroToCsvTransformer(accountDisplayNames,
                contactDisplayNames, false);

        List<String> fields = transformer.getFieldNames();
        Assert.assertEquals(fields.size(), 26);
    }

    @Test(groups = "unit", dependsOnMethods = "testS3ExportFields")
    public void testDefaultExportFields() throws IOException {
        accountDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/default/account_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(accountDisplayNames));
        contactDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/default/contact_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(contactDisplayNames));
        RecommendationAvroToCsvTransformer transformer = new RecommendationAvroToCsvTransformer(accountDisplayNames,
                contactDisplayNames, false);

        List<String> fields = transformer.getFieldNames();
        Assert.assertEquals(fields.size(), 34);

        Set<String> fieldsSet = new HashSet<String>(fields);
        Assert.assertEquals(fields.size(), fieldsSet.size());

    }

    @Test(groups = "unit", dependsOnMethods = "testDefaultExportFields")
    public void testOrderOfFields() throws IOException {
        accountDisplayNames = ImmutableMap.of( //
                "B", "B_Name", //
                "A", "A_Name");
        contactDisplayNames = ImmutableMap.of( //
                "c", "c_Name", //
                "d", "d_Name");
        RecommendationAvroToCsvTransformer transformer = new RecommendationAvroToCsvTransformer(accountDisplayNames,
                contactDisplayNames, false);

        List<String> fields = transformer.getFieldNames();
        Assert.assertEquals(fields.size(), 4);
        Assert.assertEquals(fields.get(0), "A_Name");
        Assert.assertEquals(fields.get(1), "B_Name");
        Assert.assertEquals(fields.get(2), "c_Name");
        Assert.assertEquals(fields.get(3), "d_Name");

        accountDisplayNames = null;
        contactDisplayNames = ImmutableMap.<String, String> builder() //
                .put("ContactCity", "Contact City") //
                .put("ContactState", "Contact State") //
                .put("ContactCountry", "Contact Country") //
                .put("ContactPostalCode", "Contact Zip Code") //
                .put("Contact_Address_Street_1", "Contact Address Street 1") //
                .put("Contact_Address_Street_2", "Contact Address Street 2") //
                .put("Email", "Contact Email") //
                .put("FirstName", "Contact First Name") //
                .put("LastName", "Contact Last Name") //
                .put("PhoneNumber", "Contact Phone No.") //
                .build();

        transformer = new RecommendationAvroToCsvTransformer(accountDisplayNames, contactDisplayNames, false);

        fields = transformer.getFieldNames();
        Assert.assertEquals(fields.size(), 10);
        Assert.assertEquals(fields.get(0), "Contact City");
        Assert.assertEquals(fields.get(1), "Contact Country");
        Assert.assertEquals(fields.get(2), "Contact Zip Code");
        Assert.assertEquals(fields.get(3), "Contact State");
        Assert.assertEquals(fields.get(4), "Contact Address Street 1");
        Assert.assertEquals(fields.get(5), "Contact Address Street 2");
        Assert.assertEquals(fields.get(6), "Contact Email");
        Assert.assertEquals(fields.get(7), "Contact First Name");
        Assert.assertEquals(fields.get(8), "Contact Last Name");
        Assert.assertEquals(fields.get(9), "Contact Phone No.");
    }

    @SuppressWarnings("resource")
    private Map<String, String> readCsvIntoMap(String filePath) throws IOException {
        Map<String, String> map = new HashMap<>();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        String attributeDiplayNames = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        Scanner scanner = new Scanner(attributeDiplayNames);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            String[] values = line.split(",");
            map.put(values[0], values[1]);
        }
        return map;
    }

}
