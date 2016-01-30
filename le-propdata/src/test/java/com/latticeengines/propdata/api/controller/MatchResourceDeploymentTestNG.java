package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;

@Component
public class MatchResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private MatchProxy matchProxy;

    @Test(groups = "deployment", enabled = true)
    public void testPredefined() {

        Object[][] data = new Object[][]{
                {"chevron.com", "Chevron"}
        };

        MatchInput input = MatchResourceFunctionalTestNG.prepareSimpleMatchInput(data);
        MatchOutput output = matchProxy.match(input, true);
        Assert.assertNotNull(output);
        System.out.println(JsonUtils.serialize(output));

        output = matchProxy.match(input, false);
        Assert.assertNotNull(output);
    }

}
