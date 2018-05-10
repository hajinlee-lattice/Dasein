package com.latticeengines.datacloud.match.testframework;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;

public class TestCDLMatchUtils {

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

    public static List<ColumnMetadata> loadAccountSchema() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("cdl/Account.json.gz");
        ObjectMapper om = new ObjectMapper();
        List list;
        try {
            GZIPInputStream gis = new GZIPInputStream(is);
            list = om.readValue(gis, List.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load account schema json.", e);
        }
        return JsonUtils.convertList(list, ColumnMetadata.class);
    }

}
