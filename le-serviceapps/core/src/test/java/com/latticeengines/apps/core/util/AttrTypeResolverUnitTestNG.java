package com.latticeengines.apps.core.util;

import static org.junit.Assert.assertNotEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class AttrTypeResolverUnitTestNG {

    private ColumnMetadata internal1;
    private ColumnMetadata internal2;
    private ColumnMetadata internal3;
    private ColumnMetadata internal4;
    private ColumnMetadata internal5;
    private ColumnMetadata dataCloud;
    private ColumnMetadata curated;
    private ColumnMetadata custom1;
    private ColumnMetadata custom2;
    private ColumnMetadata custom3;

    @BeforeClass(groups = "unit")
    public void setup() {
        // Account - LatticeAccountId
        internal1 = new ColumnMetadata();
        internal1.setAttrName(InterfaceName.LatticeAccountId.name());
        internal1.setEntity(BusinessEntity.Account);
        // Account - AccountId
        internal2 = new ColumnMetadata();
        internal2.setAttrName(InterfaceName.AccountId.name());
        internal2.setEntity(BusinessEntity.Account);
        internal2.setCategory(Category.ACCOUNT_ATTRIBUTES);
        // Contact - AccountId
        internal3 = new ColumnMetadata();
        internal3.setAttrName(InterfaceName.AccountId.name());
        internal3.setEntity(BusinessEntity.Contact);
        internal3.setCategory(Category.CONTACT_ATTRIBUTES);
        // Contact - ContactId
        internal4 = new ColumnMetadata();
        internal4.setAttrName(InterfaceName.ContactId.name());
        internal4.setEntity(BusinessEntity.Contact);
        // Contact - CustomerAccountId
        internal5 = new ColumnMetadata();
        internal5.setAttrName(InterfaceName.CustomerAccountId.name());
        internal5.setEntity(BusinessEntity.Contact);
        internal5.setCategory(Category.CONTACT_ATTRIBUTES);

        dataCloud = new ColumnMetadata();
        dataCloud.setEntity(BusinessEntity.Account);

        curated = new ColumnMetadata();
        curated.setEntity(BusinessEntity.PurchaseHistory);

        custom1 = new ColumnMetadata();
        // Account - CustomerAccountId
        custom2 = new ColumnMetadata();
        custom2.setAttrName(InterfaceName.CustomerAccountId.name());
        custom2.setEntity(BusinessEntity.Account);
        custom2.setCategory(Category.ACCOUNT_ATTRIBUTES);
        // Contact - CustomerContactId
        custom3 = new ColumnMetadata();
        custom3.setAttrName(InterfaceName.CustomerContactId.name());
        custom3.setEntity(BusinessEntity.Contact);
        custom3.setCategory(Category.CONTACT_ATTRIBUTES);
    }

    @Test(groups = "unit")
    public void testType() {
        assertEquals(AttrTypeResolver.resolveType(internal1, false), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal1, true), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal2, false), AttrType.Custom);
        assertEquals(AttrTypeResolver.resolveType(internal2, true), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal3, false), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal3, true), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal4, false), AttrType.Custom);
        assertEquals(AttrTypeResolver.resolveType(internal4, true), AttrType.Internal);
        assertEquals(AttrTypeResolver.resolveType(internal5, true), AttrType.Internal);


        assertEquals(AttrTypeResolver.resolveType(dataCloud, false), AttrType.DataCloud);
        dataCloud.setCategory(Category.ACCOUNT_ATTRIBUTES);
        assertNotEquals(AttrTypeResolver.resolveType(dataCloud, false), AttrType.DataCloud);
        dataCloud.setEntity(BusinessEntity.LatticeAccount);
        assertNotEquals(AttrTypeResolver.resolveType(dataCloud, false), AttrType.DataCloud);
        dataCloud.setCategory(Category.ACCOUNT_INFORMATION);
        assertEquals(AttrTypeResolver.resolveType(dataCloud, false), AttrType.DataCloud);

        assertEquals(AttrTypeResolver.resolveType(curated, false), AttrType.Curated);
        curated.setEntity(BusinessEntity.Rating);
        assertEquals(AttrTypeResolver.resolveType(curated, false), AttrType.Curated);

        assertEquals(AttrTypeResolver.resolveType(custom1, false), AttrType.Custom);
        assertEquals(AttrTypeResolver.resolveType(custom2, false), AttrType.Custom);
        assertEquals(AttrTypeResolver.resolveType(custom3, false), AttrType.Custom);
    }

    @Test(groups = "unit", dependsOnMethods = { "testType" })
    public void testSubType() {
        assertNull(AttrTypeResolver.resolveSubType(internal1, false));
        assertNull(AttrTypeResolver.resolveSubType(internal1, true));
        assertEquals(AttrTypeResolver.resolveSubType(internal2, false), AttrSubType.Standard);
        assertNull(AttrTypeResolver.resolveSubType(internal2, true));
        assertNull(AttrTypeResolver.resolveSubType(internal3, false));
        assertNull(AttrTypeResolver.resolveSubType(internal3, true));
        assertEquals(AttrTypeResolver.resolveSubType(internal4, false), AttrSubType.Standard);
        assertNull(AttrTypeResolver.resolveSubType(internal4, true));
        assertNull(AttrTypeResolver.resolveSubType(internal5, true));

        dataCloud.setCanInternalEnrich(Boolean.TRUE);
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud, false), AttrSubType.InternalEnrich);
        dataCloud.setDataLicense("HG");
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud, false), AttrSubType.Premium);
        dataCloud.setCanInternalEnrich(Boolean.FALSE);
        dataCloud.setDataLicense("");
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud, false), AttrSubType.Normal);

        curated.setEntity(BusinessEntity.Rating);
        assertEquals(AttrTypeResolver.resolveSubType(curated, false), AttrSubType.Rating);
        curated.setEntity(BusinessEntity.PurchaseHistory);
        assertEquals(AttrTypeResolver.resolveSubType(curated, false), AttrSubType.ProductBundle);

        custom1.setAttrName("Website");
        custom1.setEntity(BusinessEntity.Account);
        custom1.setCategory(Category.ACCOUNT_ATTRIBUTES);
        assertEquals(AttrTypeResolver.resolveSubType(custom1, false), AttrSubType.Standard);
        custom1.setAttrName("123");
        assertEquals(AttrTypeResolver.resolveSubType(custom1, false), AttrSubType.Extension);
        Map<ColumnSelection.Predefined, Boolean> groups = new HashMap<>();
        groups.put(ColumnSelection.Predefined.LookupId, Boolean.TRUE);
        custom1.setGroups(groups);
        assertEquals(AttrTypeResolver.resolveSubType(custom1, false), AttrSubType.LookupId);
        assertEquals(AttrTypeResolver.resolveSubType(custom2, true), AttrSubType.Standard);
        assertEquals(AttrTypeResolver.resolveSubType(custom3, true), AttrSubType.Standard);

    }
}
