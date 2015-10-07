package com.latticeengines.admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;

public class FeatureFlagResourceTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private FeatureFlagService featureFlagService;

     @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        featureFlagService.undefineFlag(FLAG_ID);
    }

    @Test(groups = "functional")
    public void defineAndRemoveFlag() {
        loginAD();
        String url = getRestHostPort() + "/admin/featureflags/" + FLAG_ID;
        ResponseDocument<?> response = restTemplate.postForObject(url, FLAG_DEFINITION,
                ResponseDocument.class);
        Assert.assertTrue(response.isSuccess(), "should be able to define a new flag.");

        url = getRestHostPort() + "/admin/featureflags";
        FeatureFlagDefinitionMap flagMap = restTemplate.getForObject(url, FeatureFlagDefinitionMap.class);
        Assert.assertTrue(flagMap.containsKey(FLAG_ID));

        url = getRestHostPort() + "/admin/featureflags/" + FLAG_ID;
        restTemplate.delete(url);

        url = getRestHostPort() + "/admin/featureflags";
        flagMap = restTemplate.getForObject(url, FeatureFlagDefinitionMap.class);
        Assert.assertFalse(flagMap.containsKey(FLAG_ID));
    }
}
