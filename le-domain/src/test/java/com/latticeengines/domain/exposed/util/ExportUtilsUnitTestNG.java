package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ExportUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testGetDisplayNameMap() {
        List<ColumnMetadata> columnMetadataList = buildMetadataList();
        Map<String, String> displayNameMap = ExportUtils.getDisplayNameMap(ExportEntity.AccountContact, columnMetadataList);
        assertEquals(displayNameMap.size(), 22);
        assertEquals(displayNameMap.get(InterfaceName.AccountId.name()), "AccountId");
        assertTrue(displayNameMap.containsValue("My Attributes_Country"));
        assertTrue(displayNameMap.containsValue("My Attributes_Country(2)"));
        assertTrue(displayNameMap.containsValue("My Attributes_Country(3)"));

        assertEquals(displayNameMap.get("MAILING_STATE_PROVINCE_NAME"), "Mailing State/Province");
        assertTrue(displayNameMap.containsValue("Firmographics_Country"));
        assertTrue(displayNameMap.containsValue("Firmographics_Country(2)"));

        assertEquals(displayNameMap.get("DESTINATION_SYS_NAME"), "Campaign Launch System Name");
        assertTrue(displayNameMap.containsValue("Country"));
        assertTrue(displayNameMap.containsValue("Country(2)"));
        assertTrue(displayNameMap.containsValue("Country(3)"));

        assertEquals(displayNameMap.get("Account__EntityCreatedSource"), "Lattice - Created Source");
        assertTrue(displayNameMap.containsValue("Curated Account Attributes_Country"));
        assertTrue(displayNameMap.containsValue("Curated Account Attributes_Country(2)"));

        assertEquals(displayNameMap.get(ExportUtils.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name()), "ContactId");
        assertTrue(displayNameMap.containsValue("Contact Attributes_Country"));
        assertTrue(displayNameMap.containsValue("Contact Attributes_Country(2)"));
        assertTrue(displayNameMap.containsValue("Contact Attributes_Country(3)"));

        assertEquals(displayNameMap.get(ExportUtils.CONTACT_ATTR_PREFIX + "am_cmba__36__w_1_w"), "Last 1 week");
        assertTrue(displayNameMap.containsValue("My Account Marketing Activity_Country"));
        assertTrue(displayNameMap.containsValue("My Account Marketing Activity_Country(2)"));
        assertTrue(displayNameMap.containsValue("My Account Marketing Activity_Country(3)"));

        assertEquals(displayNameMap.keySet().stream().filter(key -> key.startsWith(ExportUtils.CONTACT_ATTR_PREFIX)).collect(Collectors.toList()).size(), 8);
    }

    private List<ColumnMetadata> buildMetadataList() {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.ACCOUNT_ATTRIBUTES, InterfaceName.AccountId.name(), "AccountId"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.ACCOUNT_ATTRIBUTES, InterfaceName.Country.name(), "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.ACCOUNT_ATTRIBUTES, InterfaceName.City.name(), "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.ACCOUNT_ATTRIBUTES, InterfaceName.Website.name(), "Country"));

        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.FIRMOGRAPHICS, "MAILING_STATE_PROVINCE_NAME", "Mailing State/Province"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.FIRMOGRAPHICS, "LDC_Country", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, Category.FIRMOGRAPHICS, "LDC_City", "Country"));

        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, null, "DESTINATION_SYS_NAME", "Campaign Launch System Name"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, null, "PLAY_NAME", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, null, "RATING_MODEL_NAME", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Account, null, "SEGMENT_NAME", "Country"));

        columnMetadataList.add(buildColumnMetadata(BusinessEntity.CuratedAccount, Category.CURATED_ACCOUNT_ATTRIBUTES, "Account__EntityCreatedSource", "Lattice - Created Source"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.CuratedAccount, Category.CURATED_ACCOUNT_ATTRIBUTES, "Account__EntityLastUpdatedDate", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.CuratedAccount, Category.CURATED_ACCOUNT_ATTRIBUTES, "Account__EntityCreatedDate", "Country"));

        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Contact, Category.CONTACT_ATTRIBUTES, InterfaceName.ContactId.name(), "ContactId"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Contact, Category.CONTACT_ATTRIBUTES, InterfaceName.CompanyName.name(), "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Contact, Category.CONTACT_ATTRIBUTES, InterfaceName.Email.name(), "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.Contact, Category.CONTACT_ATTRIBUTES, InterfaceName.Website.name(), "Country"));

        columnMetadataList.add(buildColumnMetadata(BusinessEntity.ContactMarketingActivity, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE, "am_cmba__36__w_1_w", "Last 1 week"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.ContactMarketingActivity, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE, "am_cmba__36__w_2_w", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.ContactMarketingActivity, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE, "am_cmba__36__w_4_w", "Country"));
        columnMetadataList.add(buildColumnMetadata(BusinessEntity.ContactMarketingActivity, Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE, "am_cmba__36__w_8_w", "Country"));
        return columnMetadataList;
    }

    protected ColumnMetadata buildColumnMetadata(BusinessEntity entity, Category category, String attributeName, String displayName) {
        ColumnMetadata cm = new ColumnMetadata(attributeName, String.class.getSimpleName());
        cm.setCategory(category);
        cm.setDisplayName(displayName);
        cm.setEntity(entity);
        return cm;
    }
}
