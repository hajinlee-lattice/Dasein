package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class DataFeedTaskManagerServiceImplUnitTestNG {

    private DataFeedTaskManagerServiceImpl dataFeedTaskManagerServiceImpl = new DataFeedTaskManagerServiceImpl(null,
            null, null, null);

    @Test(groups = "unit")
    public void testUpdateTableAttrName() {
        Table templateTable = new Table();
        Attribute attribute1 = new Attribute("AccountId");
        Attribute attribute2 = new Attribute("TestAttr");
        templateTable.addAttributes(Arrays.asList(attribute1, attribute2));
        Table metaTable = new Table();
        Attribute attribute3 = new Attribute("AccountId");
        Attribute attribute4 = new Attribute("testATTR");
        metaTable.addAttributes(Arrays.asList(attribute3, attribute4));
        Assert.assertNotNull(metaTable.getAttribute("testATTR"));
        Assert.assertNotNull(metaTable.getAttribute("AccountId"));
        Assert.assertNull(metaTable.getAttribute("TestAttr"));
        dataFeedTaskManagerServiceImpl.updateTableAttributeName(templateTable, metaTable);
        Assert.assertNull(metaTable.getAttribute("testATTR"));
        Assert.assertNotNull(metaTable.getAttribute("AccountId"));
        Assert.assertNotNull(metaTable.getAttribute("TestAttr"));
    }
}
