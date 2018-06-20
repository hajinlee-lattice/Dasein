package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;


public class AttrConfigResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AttrConfigResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        cdlTestDataService.populateData(mainTestTenant.getId());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", dataProvider = "Categories")
    public void testGetStats(String catDisplayName, String subcat) {
        log.info(String.format("Test category = %s, sub-cateogry = %s", catDisplayName, subcat));
        Map map = restTemplate.getForObject(
                getRestAPIHostPort()
                        + String.format("/pls/attrconfig/stats/category/%s?subcategory=%s", catDisplayName, subcat),
                Map.class);
        Map<String, AttributeStats> stats = JsonUtils.convertMap(map, String.class, AttributeStats.class);
        Assert.assertTrue(MapUtils.isNotEmpty(stats));
    }

    // TODO: Prepared metadata in cdlTestDataService only has 2 categories.
    // Could improve test artifact later
    @DataProvider(name = "Categories")
    public Object[][] getCategories() {
        return new Object[][] {
                { Category.CONTACT_ATTRIBUTES.name(), "Other" }, //
                { Category.ACCOUNT_ATTRIBUTES.name(), "Other" }, //
        };
    }

}
