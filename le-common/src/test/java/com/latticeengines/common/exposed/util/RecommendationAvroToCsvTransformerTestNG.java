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

import org.apache.avro.Schema;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.transformer.RecommendationAvroToCsvTransformer;

public class RecommendationAvroToCsvTransformerTestNG {
    private static Logger log = LoggerFactory.getLogger(RecommendationAvroToCsvTransformerTestNG.class);

    private Map<String, String> accountDisplayNames;
    private Map<String, String> contactDisplayNames;
    private Schema schema;

    @BeforeClass(groups = "unit")
    public void setup() throws IOException {
        accountDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/account_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(accountDisplayNames));
        contactDisplayNames = readCsvIntoMap("com/latticeengines/play/launch/contact_display_names.csv");
        Assert.assertTrue(MapUtils.isNotEmpty(contactDisplayNames));
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/play/launch/testLaunchAvro.avro");
        Assert.assertNotNull(is);
        schema = AvroUtils.readSchemaFromInputStream(is);
    }

    @Test(groups = "unit")
    public void testS3ExportFields() {
        RecommendationAvroToCsvTransformer transformer = new RecommendationAvroToCsvTransformer(accountDisplayNames,
                contactDisplayNames, false);

        List<String> fields = transformer.getFieldNames(schema);
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

        List<String> fields = transformer.getFieldNames(schema);
        Assert.assertEquals(fields.size(), 34);

        Set<String> fieldsSet = new HashSet<String>(fields);
        Assert.assertEquals(fields.size(), fieldsSet.size());

    }

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
