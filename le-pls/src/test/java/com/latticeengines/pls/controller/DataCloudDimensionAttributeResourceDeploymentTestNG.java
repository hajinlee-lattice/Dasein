package com.latticeengines.pls.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class DataCloudDimensionAttributeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Test(groups = "deployment")
    @SuppressWarnings("unchecked")
    public void getAllDimensionAttributes() throws Exception {
        List<CategoricalDimension> dimensions = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dimensionattributes/dimensions", List.class);
        ObjectMapper objMapper = new ObjectMapper();
        dimensions = objMapper.convertValue(dimensions, new TypeReference<List<CategoricalDimension>>() { });
        Assert.assertTrue(dimensions.size() > 0);

        Long rootAttrId = dimensions.get(0).getRootAttrId();
        
        List<CategoricalAttribute> attributes = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dimensionattributes/attributes?rootId="
                        + rootAttrId, List.class);
        Assert.assertTrue(attributes.size() > 0);
    }

}
