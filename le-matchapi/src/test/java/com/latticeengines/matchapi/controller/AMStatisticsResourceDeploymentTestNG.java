package com.latticeengines.matchapi.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes.TopAttribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

public class AMStatisticsResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(AMStatisticsResourceDeploymentTestNG.class);

    @Test(groups = { "deployment" }, enabled = false)
    public void testGetTopAttrTree() {
        TopNAttributeTree topNAttributeTree = amStatsProxy.getTopAttrTree();
        Assert.assertNotNull(topNAttributeTree);
        Assert.assertNotNull(topNAttributeTree.get(Category.WEBSITE_PROFILE));
        Assert.assertTrue(topNAttributeTree.get(Category.WEBSITE_PROFILE).getTopAttributes().size() > 0);
        Map<String, List<TopAttribute>> topAttributes = topNAttributeTree.get(Category.WEBSITE_PROFILE)
                .getTopAttributes();
        for (String subCategory : topAttributes.keySet()) {
            log.info(String.format("SubCategory: %s", subCategory));
            List<TopAttribute> attributes = topAttributes.get(subCategory);
            for (TopAttribute attribute : attributes) {
                log.info(String.format("Attribute: %s, Count: %d", attribute.getAttribute(),
                        attribute.getNonNullCount()));
            }
        }
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void testGetTopCube() {
        AccountMasterCube cube = amStatsProxy.getCube(createQuery());
        Assert.assertNotNull(cube);
        Assert.assertNotNull(cube);
        Assert.assertNotNull(cube.getStatistics());
        Assert.assertTrue(cube.getStatistics().size() > 0);
        for (String attribute : cube.getStatistics().keySet()) {
            Assert.assertNotNull(cube.getStatistics().get(attribute));
        }
    }

    private AccountMasterFactQuery createQuery() {
        AccountMasterFactQuery query = new AccountMasterFactQuery();
        DimensionalQuery locationQry = new DimensionalQuery();
        locationQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        locationQry.setDimension(DataCloudConstants.DIMENSION_LOCATION);
        Map<String, String> qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_COUNTRY, CategoricalAttribute.ALL);
        locationQry.setQualifiers(qualifiers);
        query.setLocationQry(locationQry);
        DimensionalQuery industryQry = new DimensionalQuery();
        industryQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        industryQry.setDimension(DataCloudConstants.DIMENSION_INDUSTRY);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_INDUSTRY, CategoricalAttribute.ALL);
        industryQry.setQualifiers(qualifiers);
        query.setIndustryQry(industryQry);
        DimensionalQuery numEmpRangeQry = new DimensionalQuery();
        numEmpRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        numEmpRangeQry.setDimension(DataCloudConstants.DIMENSION_NUM_EMP_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_NUM_EMP_RANGE, CategoricalAttribute.ALL);
        numEmpRangeQry.setQualifiers(qualifiers);
        query.setNumEmpRangeQry(numEmpRangeQry);
        DimensionalQuery revRangeQry = new DimensionalQuery();
        revRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        revRangeQry.setDimension(DataCloudConstants.DIMENSION_REV_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_REV_RANGE, CategoricalAttribute.ALL);
        revRangeQry.setQualifiers(qualifiers);
        query.setRevRangeQry(revRangeQry);
        DimensionalQuery numLocRangeQry = new DimensionalQuery();
        numLocRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        numLocRangeQry.setDimension(DataCloudConstants.DIMENSION_NUM_LOC_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_NUM_LOC_RANGE, CategoricalAttribute.ALL);
        numLocRangeQry.setQualifiers(qualifiers);
        query.setNumLocRangeQry(numLocRangeQry);
        DimensionalQuery categoryQry = new DimensionalQuery();
        categoryQry.setSource(DataCloudConstants.ACCOUNT_MASTER_COLUMN);
        categoryQry.setDimension(DataCloudConstants.DIMENSION_CATEGORY);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_CATEGORY, CategoricalAttribute.ALL);
        categoryQry.setQualifiers(qualifiers);
        query.setCategoryQry(categoryQry);
        return query;
    }
}
