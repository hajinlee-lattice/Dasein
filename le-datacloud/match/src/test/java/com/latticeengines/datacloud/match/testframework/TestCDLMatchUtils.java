package com.latticeengines.datacloud.match.testframework;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class TestCDLMatchUtils {

    protected TestCDLMatchUtils() {
        throw new UnsupportedOperationException();
    }

    public static DynamoDataUnit mockCustomAccount() {
        DynamoDataUnit dynamoDataUnit = new DynamoDataUnit();
        dynamoDataUnit.setName("localtest_account_4");
        dynamoDataUnit.setTenant("LocalTest");
        dynamoDataUnit.setSignature("20180425");
        dynamoDataUnit.setLinkedTable("Account_2018-05-07_19-05-23_UTC");
        dynamoDataUnit.setLinkedTenant("LETest1525719796735");
        dynamoDataUnit.setPartitionKey("AccountId");
        return dynamoDataUnit;
    }

    private static String[][] dynamoDataUnits = { //
            { "Account_2018-06-22_23-04-26_UTC", "LocalTest", "20180425", "Account_2018-06-22_23-04-26_UTC",
                    "LETest1533755623454", "AccountId" }, //
            { "LETest1533755623454_Rating_2018_08_08_22_06_57_UTC", "LocalTest", "20180425",
                    "LETest1533755623454_Rating_2018_08_08_22_06_57_UTC", "LETest1533755623454", "AccountId" }, //
            { "LETest1533755623454_PurchaseHistory_2018_08_08_21_21_59_UTC", "LocalTest", "20180425",
                    "LETest1533755623454_PurchaseHistory_2018_08_08_21_21_59_UTC", "LETest1533755623454", "AccountId" }, //
    };

    public static List<DynamoDataUnit> mockDynamoDataUnits() {
        List<DynamoDataUnit> toReturn = new ArrayList<>();
        for (String[] unit : dynamoDataUnits) {
            DynamoDataUnit dynamoDataUnit = new DynamoDataUnit();
            dynamoDataUnit.setName(unit[0]);
            dynamoDataUnit.setTenant(unit[1]);
            dynamoDataUnit.setSignature(unit[2]);
            dynamoDataUnit.setLinkedTable(unit[3]);
            dynamoDataUnit.setLinkedTenant(unit[4]);
            dynamoDataUnit.setPartitionKey(unit[5]);
            toReturn.add(dynamoDataUnit);
        }
        return toReturn;
    }

    public static List<ColumnMetadata> loadAccountSchema(BusinessEntity entity) {
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(String.format("cdl/%s.json.gz", entity.name()));
        ObjectMapper om = new ObjectMapper();
        List<?> list;
        try {
            GZIPInputStream gis = new GZIPInputStream(is);
            list = om.readValue(gis, List.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load decorated metadata json file.", e);
        }
        return JsonUtils.convertList(list, ColumnMetadata.class);
    }
}
