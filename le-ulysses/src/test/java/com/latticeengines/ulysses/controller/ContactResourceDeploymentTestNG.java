package com.latticeengines.ulysses.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class ContactResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testGetContactsByAccountId() {
        DataPage responseData = getOAuth2RestTemplate()
                .getForObject(getUlyssesRestAPIPort() + "/ulysses/contacts/accounts/testId", DataPage.class);
        Assert.assertNotNull(responseData);
        Assert.assertNotNull(responseData.getData());
        Assert.assertEquals(25, responseData.getData().size());
    }
}
