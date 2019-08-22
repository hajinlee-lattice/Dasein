package com.latticeengines.apps.cdl.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.map.HashedMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ConvertBatchStoreInfoService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

public class ConvertBatchStoreInfoServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ConvertBatchStoreInfoService convertBatchStoreInfoService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testConvertBatchStoreInfoService() {
        ConvertBatchStoreInfo info = convertBatchStoreInfoService.create(mainCustomerSpace);
        Assert.assertNotNull(info.getPid());
        Assert.assertNull(info.getConvertDetail());
        ConvertBatchStoreDetail detail = new ConvertBatchStoreDetail();
        detail.setTaskUniqueId("UniqueId");
        Map<String, String> renameMap = new HashedMap<>();
        renameMap.put("OriginalName", "NewName");
        detail.setRenameMap(renameMap);
        convertBatchStoreInfoService.updateDetails(mainCustomerSpace, info.getPid(), detail);
        info = convertBatchStoreInfoService.getByPid(mainCustomerSpace, info.getPid());
        Assert.assertNotNull(info);
        Assert.assertNotNull(info.getConvertDetail());
        Assert.assertEquals(info.getConvertDetail().getTaskUniqueId(), "UniqueId");
        Assert.assertNotNull(info.getConvertDetail().getRenameMap());
        Assert.assertTrue(info.getConvertDetail().getRenameMap().containsKey("OriginalName"));
        Assert.assertEquals(info.getConvertDetail().getRenameMap().get("OriginalName"), "NewName");

    }
}
