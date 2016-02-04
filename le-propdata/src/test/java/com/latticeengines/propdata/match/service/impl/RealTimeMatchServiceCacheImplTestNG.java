package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;

@Component
public class RealTimeMatchServiceCacheImplTestNG extends PropDataMatchFunctionalTestNGBase {

    @SuppressWarnings("unused")
    @Autowired
    private RealTimeMatchServiceCacheImpl matchService;

    @Test(groups = "functional")
    public void testSimpleMatch() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = matchService.match(input, true);
        Assert.assertNotNull(output);
    }
}
