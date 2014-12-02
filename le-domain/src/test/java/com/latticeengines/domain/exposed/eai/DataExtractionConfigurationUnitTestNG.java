package com.latticeengines.domain.exposed.eai;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataExtractionConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        List<Table> tables = new ArrayList<>();
        Table activity = createActivity();
        tables.add(activity);
        DataExtractionConfiguration extractionConfig = new DataExtractionConfiguration();
        extractionConfig.setName("Extraction-" + System.currentTimeMillis());
        extractionConfig.setTables(tables);
        extractionConfig.putFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");

        String extractionConfigStr = extractionConfig.toString();
        System.out.println(extractionConfigStr);

        DataExtractionConfiguration deserializedExtractionConfig = JsonUtils.deserialize(extractionConfigStr,
                DataExtractionConfiguration.class);
        assertEquals(deserializedExtractionConfig.toString(), extractionConfigStr);
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
        return table;
    }

}
