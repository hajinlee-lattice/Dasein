package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchKey;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;


public class MatchResourceFunctionalTestNG extends PropDataApiFunctionalTestNGBase {
    private static final String MATCH_ENDPOINT = "propdata/matches";

    @Test(groups = { "api" }, enabled = true)
    public void testPredefined() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][]{
                {"chevron.com", "Chevron"}
        };

        MatchInput input = prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);

        System.out.println(JsonUtils.serialize(output));
    }

    static MatchInput prepareSimpleMatchInput(Object[][] data) {
        MatchInput input = new MatchInput();
        input.setMatchEngine(MatchInput.MatchEngine.RealTime);
        input.setTenant(new Tenant("PD_Test"));
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Name));
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row: data) {
            mockData.add(Arrays.asList(row));
        }
        input.setData(mockData);
        return input;
    }

}
