package com.latticeengines.eai.service.impl;

import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;

public class EaiMetadataServiceImplUnitTestNG {

    @Test(groups = "Unit")
    public void addExtractTable() {
        EaiMetadataServiceImpl eaiMetadataService = new EaiMetadataServiceImpl();
        Table table = new Table();
        String path = "hdfs://localhost:9000/Pods/Default/Contracts/Salesforce-Eai/Tenants/Salesforce-Eai/Spaces/Production/Data/Tables/Lead/Extracts/2015-10-12-15-09-36/Lead-2015-10-12.avro";
        eaiMetadataService.addExtractToTable(table, path);
        System.out.println(JsonUtils.serialize(table.getExtracts().get(0)));
        assertTrue(table.getExtracts().get(0).getPath().contains("Lead-2015-10-12.avro"));
        assertTrue(table.getExtracts().get(0).getPath().startsWith("/Pods"));
        assertTrue(table.getExtracts().get(0).getExtractionTimestamp() > 0);
        assertEquals(table.getExtracts().get(0).getName(), "Lead-2015-10-12.avro");
    }
}
