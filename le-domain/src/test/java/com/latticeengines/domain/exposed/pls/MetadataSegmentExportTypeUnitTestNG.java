package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MetadataSegmentExportTypeUnitTestNG {

    @Test(groups = "unit")
    public void testAccountType() {
        List<Pair<String, String>> fieldNamePairs = MetadataSegmentExportType.ACCOUNT.getFieldNamePairs();
        Assert.assertTrue(CollectionUtils.isNotEmpty(fieldNamePairs));
        fieldNamePairs.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getLeft()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
            Assert.assertTrue(p.getLeft().contains("_"));
            Assert.assertNotNull(InterfaceName.valueOf(p.getLeft().substring(p.getLeft().indexOf("_") + 1)));
        });
        Assert.assertEquals(fieldNamePairs.size(), 9);
    }

    @Test(groups = "unit")
    public void testContactType() {
        List<Pair<String, String>> fieldNamePairs = MetadataSegmentExportType.CONTACT.getFieldNamePairs();
        Assert.assertTrue(CollectionUtils.isNotEmpty(fieldNamePairs));
        fieldNamePairs.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getLeft()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
            Assert.assertTrue(p.getLeft().contains("_"));
            Assert.assertNotNull(InterfaceName.valueOf(p.getLeft().substring(p.getLeft().indexOf("_") + 1)));
        });
        Assert.assertEquals(fieldNamePairs.size(), 5);
    }

    @Test(groups = "unit")
    public void testAccountAndContactType() {
        List<Pair<String, String>> fieldNamePairs = MetadataSegmentExportType.ACCOUNT_AND_CONTACT.getFieldNamePairs();
        Assert.assertTrue(CollectionUtils.isNotEmpty(fieldNamePairs));
        fieldNamePairs.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getLeft()));
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
            Assert.assertTrue(p.getLeft().contains("_"));
            Assert.assertNotNull(InterfaceName.valueOf(p.getLeft().substring(p.getLeft().indexOf("_") + 1)));
        });
        Assert.assertEquals(fieldNamePairs.size(), 12);
    }

    @Test(groups = "unit")
    public void testAccountIdType() {
        List<Pair<String, String>> fieldNamePairs = MetadataSegmentExportType.ACCOUNT_ID.getFieldNamePairs();
        Assert.assertTrue(CollectionUtils.isNotEmpty(fieldNamePairs));
        fieldNamePairs.stream().forEach(p -> {
            Assert.assertNotNull(p);
            Assert.assertNotNull(p.getLeft());
            Assert.assertNotNull(p.getRight());
            Assert.assertTrue(StringUtils.isNotBlank(p.getRight()));
            Assert.assertNotNull(InterfaceName.valueOf(p.getLeft()));
        });
        Assert.assertEquals(fieldNamePairs.size(), 1);
    }

    @Test(groups = "unit")
    public void getDefaultExportAttributesAccount() {
        Set<InterfaceName> defaultExportAttributes = MetadataSegmentExportType
                .getDefaultExportAttributes(BusinessEntity.Account);
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultExportAttributes));
        defaultExportAttributes.stream().forEach(p -> {
            Assert.assertNotNull(p);
        });
        Assert.assertEquals(defaultExportAttributes.size(), 9);
    }

    @Test(groups = "unit")
    public void getDefaultExportAttributesContact() {
        Set<InterfaceName> defaultExportAttributes = MetadataSegmentExportType
                .getDefaultExportAttributes(BusinessEntity.Contact);
        Assert.assertTrue(CollectionUtils.isNotEmpty(defaultExportAttributes));
        defaultExportAttributes.stream().forEach(p -> {
            Assert.assertNotNull(p);
        });
        Assert.assertEquals(defaultExportAttributes.size(), 5);
    }
}
