package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;
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
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
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

    @Test(groups = "deployment")
    public void testUpdateName() {
        log.info("test update attrconfig name for {}", Category.CONTACT_ATTRIBUTES.name());
        String url = getRestAPIHostPort()
                + String.format("/pls/attrconfig/name/config/category/%s", Category.CONTACT_ATTRIBUTES.name());
        AttrConfigSelectionDetail.SubcategoryDetail subcategoryDetail = restTemplate.getForObject(url,
                AttrConfigSelectionDetail.SubcategoryDetail.class);
        List<AttrConfigSelectionDetail.AttrDetail> details = subcategoryDetail.getAttributes();
        Assert.assertTrue(details.size() > 2);
        // get two details from get api and assign the display name of the second to the first one
        AttrConfigSelectionDetail.AttrDetail detail1 = details.get(0);
        AttrConfigSelectionDetail.AttrDetail detail2 = details.get(1);
        detail1.setDisplayName(detail2.getDisplayName());
        AttrConfigSelectionDetail.SubcategoryDetail requestBody = new AttrConfigSelectionDetail.SubcategoryDetail();
        requestBody.setAttributes(Collections.singletonList(detail1));
        HttpEntity<AttrConfigSelectionDetail.SubcategoryDetail> entities = new HttpEntity<>(requestBody,
                new HttpHeaders());
        ResponseEntity<AttrConfigSelectionDetail.SubcategoryDetail> responseBody = restTemplate.exchange(url,
                HttpMethod.PUT, entities,
                AttrConfigSelectionDetail.SubcategoryDetail.class);
        Assert.assertNotNull(responseBody.getBody());
        AttrConfigSelectionDetail.SubcategoryDetail categoryDetailResult = responseBody.getBody();
        List<AttrConfigSelectionDetail.AttrDetail> resultDetails = categoryDetailResult.getAttributes();
        Assert.assertNotNull(resultDetails);
        Assert.assertEquals(resultDetails.size(), 1);
        AttrConfigSelectionDetail.AttrDetail resultDetail = resultDetails.get(0);
        Assert.assertEquals(resultDetail.getErrorMessage(), ValidationMsg.Errors.DUPLICATED_NAME);



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
