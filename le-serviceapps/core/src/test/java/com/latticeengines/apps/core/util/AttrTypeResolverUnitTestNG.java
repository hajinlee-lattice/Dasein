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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class AttrTypeResolverUnitTestNG {

    private ColumnMetadata internal;
    private ColumnMetadata dataCloud;
    private ColumnMetadata curated;
    private ColumnMetadata custom;
    @BeforeClass(groups = "unit")
    public void setup() {
        internal = new ColumnMetadata();
        internal.setAttrName("LatticeAccountId");
        internal.setEntity(BusinessEntity.Account);
        dataCloud = new ColumnMetadata();
        dataCloud.setEntity(BusinessEntity.Account);
        curated = new ColumnMetadata();
        curated.setEntity(BusinessEntity.PurchaseHistory);
        custom = new ColumnMetadata();
    }

    @Test(groups = "unit")
    public void testType() {
        assertEquals(AttrTypeResolver.resolveType(internal), AttrType.Internal);

        assertEquals(AttrTypeResolver.resolveType(dataCloud), AttrType.DataCloud);
        dataCloud.setCategory(Category.ACCOUNT_ATTRIBUTES);
        assertNotEquals(AttrTypeResolver.resolveType(dataCloud), AttrType.DataCloud);
        dataCloud.setEntity(BusinessEntity.LatticeAccount);
        assertNotEquals(AttrTypeResolver.resolveType(dataCloud), AttrType.DataCloud);
        dataCloud.setCategory(Category.ACCOUNT_INFORMATION);
        assertEquals(AttrTypeResolver.resolveType(dataCloud), AttrType.DataCloud);

        assertEquals(AttrTypeResolver.resolveType(curated), AttrType.Curated);
        curated.setEntity(BusinessEntity.Rating);
        assertEquals(AttrTypeResolver.resolveType(curated), AttrType.Curated);

        assertEquals(AttrTypeResolver.resolveType(custom), AttrType.Custom);
    }

    @Test(groups = "unit", dependsOnMethods = { "testType" })
    public void testSubType() {
        assertNull(AttrTypeResolver.resolveSubType(internal));

        dataCloud.setCanInternalEnrich(Boolean.TRUE);
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud), AttrSubType.InternalEnrich);
        dataCloud.setDataLicense("HG");
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud), AttrSubType.Premium);
        dataCloud.setCanInternalEnrich(Boolean.FALSE);
        dataCloud.setDataLicense("");
        assertEquals(AttrTypeResolver.resolveSubType(dataCloud), AttrSubType.Normal);

        curated.setEntity(BusinessEntity.Rating);
        assertEquals(AttrTypeResolver.resolveSubType(curated), AttrSubType.Rating);
        curated.setEntity(BusinessEntity.PurchaseHistory);
        assertEquals(AttrTypeResolver.resolveSubType(curated), AttrSubType.ProductBundle);

        custom.setAttrName("Website");
        custom.setEntity(BusinessEntity.Account);
        custom.setCategory(Category.ACCOUNT_ATTRIBUTES);
        assertEquals(AttrTypeResolver.resolveSubType(custom), AttrSubType.Standard);
        custom.setAttrName("123");
        assertEquals(AttrTypeResolver.resolveSubType(custom), AttrSubType.Extension);
        Map<ColumnSelection.Predefined, Boolean> groups = new HashMap<>();
        groups.put(ColumnSelection.Predefined.LookupId, Boolean.TRUE);
        custom.setGroups(groups);
        assertEquals(AttrTypeResolver.resolveSubType(custom), AttrSubType.LookupId);
    }
}
