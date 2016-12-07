package com.latticeengines.datacloud.core.service.impl;


import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.AccountMasterStatisticsService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.metadata.Category;

public class AccountMasterStatisticsServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private CategoricalAttributeEntityMgr attributeEntityMgr;

    @Autowired
    private AccountMasterStatisticsService accountMasterStatisticsService;

    @Test(groups = "functional", enabled = false)
    public void testAMCategories() {
        Map<Category, Long> catIdMap = accountMasterStatisticsService.getCategories();
        for (Map.Entry<Category, Long> entry: catIdMap.entrySet()) {
            Category category = entry.getKey();
            Long attrId = entry.getValue();
            verifyAttribute(attrId, PropDataConstants.ATTR_CATEGORY, category.getName());
        }

        for (Category category: Category.values()) {
            Map<String, Long> subCatIdMap = accountMasterStatisticsService.getSubCategories(category);
            //TODO: verify the subCatIdMap
        }
    }

    private void verifyAttribute(Long attrId, String attrName, String attrValue) {
        CategoricalAttribute attribute = attributeEntityMgr.getAttribute(attrId);
        Assert.assertNotNull(attribute);
        Assert.assertEquals(attribute.getAttrName(), attrName);
        Assert.assertEquals(attribute.getAttrValue(), attrValue);
    }

}
