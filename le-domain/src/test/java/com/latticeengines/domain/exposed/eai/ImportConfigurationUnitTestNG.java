package com.latticeengines.domain.exposed.eai;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class ImportConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        List<Table> tables = new ArrayList<>();
        Table activity = createActivity();
        tables.add(activity);
        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setName("Extraction-" + System.currentTimeMillis());
        importConfig.setCustomerSpace(CustomerSpace.parse("C1.C1.Production"));
        
        SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
        marketoImportConfig.setSourceType(SourceType.MARKETO);
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
        marketoImportConfig.setProperty("a", "a_value");
        
        importConfig.addSourceConfiguration(marketoImportConfig);

        String importConfigStr = importConfig.toString();
        System.out.println(importConfigStr);

        ImportConfiguration deserializedImportConfig = JsonUtils.deserialize(importConfigStr,
                ImportConfiguration.class);
        assertEquals(deserializedImportConfig.toString(), importConfigStr);
    }

    private Table createActivity() {
        Table table = new Table();
        table.setName("Activity");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("id");
        Attribute leadId = new Attribute();
        leadId.setName("leadId");
        leadId.setDisplayName("Lead Id");
        leadId.setLogicalDataType("integer");
        Attribute activityDate = new Attribute();
        activityDate.setName("activityDate");
        activityDate.setDisplayName("Activity Date");
        activityDate.setLogicalDataType("datetime");
        Attribute activityTypeId = new Attribute();
        activityTypeId.setName("activityTypeId");
        activityTypeId.setDisplayName("Activity Type Id");
        activityTypeId.setLogicalDataType("integer");
        table.addAttribute(id);
        table.addAttribute(leadId);
        table.addAttribute(activityDate);
        table.addAttribute(activityTypeId);
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute("ID");
        table.setPrimaryKey(pk);
        return table;
    }

}
