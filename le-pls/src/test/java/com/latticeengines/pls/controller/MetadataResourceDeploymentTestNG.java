package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class MetadataResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final int PAGE_SIZE = 1000;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
    }

    @Test(groups = "deployment")
    public void testGetAttributes() {
        List<ColumnMetadata> attributes = new ArrayList<>();
        int count = restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/metadata/attributes/count",
                Integer.class);
        int offset = 0;
        while (offset < count) {
            attributes.addAll(JsonUtils.convertList(
                    restTemplate.getForObject(
                            getDeployedRestAPIHostPort()
                                    + String.format("/pls/metadata/attributes?offset=%s&max=%s", offset, PAGE_SIZE),
                            List.class), ColumnMetadata.class));
            offset += PAGE_SIZE;
        }
        assertEquals(attributes.size(), count);
    }

    @Test(groups = "deployment", expectedExceptions = Exception.class)
    public void testGetAttributesInvalidOffset() {
        restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/metadata/attributes?offset=-1&max=100",
                List.class);
    }
}
