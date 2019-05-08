package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AtlasExportTypeUnitTestNG {

    @Test(groups = "unit")
    public void testAccountType() {
        List<Triple<BusinessEntity, String, String>> defaultAttributeTuples = AtlasExportType.ACCOUNT
                .getDefaultAttributeTuples();
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultAttributeTuples));
        defaultAttributeTuples.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getMiddle());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getMiddle()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
        });
        Assert.assertEquals(defaultAttributeTuples.size(), 9);
    }

    @Test(groups = "unit")
    public void testContactType() {
        List<Triple<BusinessEntity, String, String>> defaultAttributeTuples = AtlasExportType.CONTACT
                .getDefaultAttributeTuples();
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultAttributeTuples));
        defaultAttributeTuples.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getMiddle());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getMiddle()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
        });
        Assert.assertEquals(defaultAttributeTuples.size(), 5);
    }

    @Test(groups = "unit")
    public void testAccountAndContactType() {
        List<Triple<BusinessEntity, String, String>> defaultAttributeTuples = AtlasExportType.ACCOUNT_AND_CONTACT
                .getDefaultAttributeTuples();
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultAttributeTuples));
        defaultAttributeTuples.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getMiddle());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getMiddle()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
        });
        Assert.assertEquals(defaultAttributeTuples.size(), 12);
    }

    @Test(groups = "unit")
    public void testAccountIdType() {
        List<Triple<BusinessEntity, String, String>> defaultAttributeTuples = AtlasExportType.ACCOUNT_ID
                .getDefaultAttributeTuples();
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultAttributeTuples));
        defaultAttributeTuples.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getMiddle());
            Assert.assertNotNull(p.getRight());
            Assert.assertNotNull(InterfaceName.valueOf(p.getMiddle()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
        });
        Assert.assertEquals(defaultAttributeTuples.size(), 1);
    }

    @Test(groups = "unit")
    public void getDefaultExportAttributesAccount() {
        Set<InterfaceName> defaultExportAttributes = AtlasExportType
                .getDefaultExportAttributes(BusinessEntity.Account);
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultExportAttributes));
        defaultExportAttributes.stream().forEach(p -> {
            Assert.assertNotNull(p);
        });
        Assert.assertEquals(defaultExportAttributes.size(), 9);
    }

    @Test(groups = "unit")
    public void getDefaultExportAttributesContact() {
        Set<InterfaceName> defaultExportAttributes = AtlasExportType
                .getDefaultExportAttributes(BusinessEntity.Contact);
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultExportAttributes));
        defaultExportAttributes.stream().forEach(p -> {
            Assert.assertNotNull(p);
        });
        Assert.assertEquals(defaultExportAttributes.size(), 5);
    }
}
