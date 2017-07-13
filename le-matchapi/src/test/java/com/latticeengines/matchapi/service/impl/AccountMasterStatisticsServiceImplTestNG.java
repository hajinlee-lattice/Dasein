package com.latticeengines.matchapi.service.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.matchapi.service.AccountMasterStatisticsService;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;

public class AccountMasterStatisticsServiceImplTestNG extends MatchapiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterStatisticsServiceImplTestNG.class);

    @Autowired
    private CategoricalAttributeEntityMgr attributeEntityMgr;

    @Autowired
    private AccountMasterStatisticsService accountMasterStatisticsService;

    @Test(groups = "functional", enabled = true)
    public void testAMCategories() {
        Map<Category, Long> catIdMap = accountMasterStatisticsService.getCategories();
        for (Map.Entry<Category, Long> entry : catIdMap.entrySet()) {
            Category category = entry.getKey();
            Long attrId = entry.getValue();
            if (attrId != null) {
                verifyAttribute(attrId, DataCloudConstants.ATTR_CATEGORY, category.name());
            }
        }

        for (Category category : Category.values()) {
            Map<String, Long> subCatIdMap = accountMasterStatisticsService.getSubCategories(category);
            log.info(String.format("Category: %s, Size: %d", category, subCatIdMap.size()));
            // This part might be broken if there is any change to category,
            // subcategory and enrichment tag in metadata
            switch (category) {
            case DEFAULT:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case FIRMOGRAPHICS:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case GROWTH_TRENDS:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case INTENT:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case LEAD_INFORMATION:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case ONLINE_PRESENCE:
                Assert.assertEquals(subCatIdMap.size(), 1);
                break;
            case TECHNOLOGY_PROFILE:
                Assert.assertEquals(subCatIdMap.size(), 92);
                break;
            case WEBSITE_KEYWORDS:
                Assert.assertEquals(subCatIdMap.size(), 8);
                break;
            case WEBSITE_PROFILE:
                Assert.assertEquals(subCatIdMap.size(), 184);
                break;
            default:
                break;
            }
            for (String subCat : subCatIdMap.keySet()) {
                Assert.assertNotNull(subCat);
                Assert.assertNotNull(subCatIdMap.get(subCat));
            }
        }
    }

    private void verifyAttribute(Long attrId, String attrName, String attrValue) {
        CategoricalAttribute attribute = attributeEntityMgr.getAttribute(attrId);
        Assert.assertNotNull(attribute);
        Assert.assertEquals(attribute.getAttrName(), attrName);
        Assert.assertEquals(attribute.getAttrValue(), attrValue);
    }

}
