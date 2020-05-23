package com.latticeengines.domain.exposed.datacloud.match.config;

import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Country;
import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.NonHeadQuarters;
import static com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion.OutOfBusiness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DplusMatchConfigUnitTestNG {

    @Test(groups = "unit")
    public void testBuilder() {
        DplusMatchRule baseRule = new DplusMatchRule(7) // code >= 7
                .exclude(OutOfBusiness) //
                .review(4, 6, Arrays.asList("A", "B")); // code in [4, 6] or grade is A|B
        DplusMatchRule ukRule = //
                new DplusMatchRule(8, Arrays.asList("A", "B")) // code >= 8 or grade is A|B
                        .exclude(OutOfBusiness, NonHeadQuarters) //
                        .review(4, 7, Collections.singleton("A")); // code in [4, 7] or grade is A
        DplusMatchConfig config = new DplusMatchConfig(baseRule) //
                .when(Country, Collections.singleton("UK")).apply(ukRule);

        String serializedStr = JsonUtils.serialize(config);
        System.out.println(serializedStr);

        DplusMatchConfig deserialized = JsonUtils.deserialize(serializedStr, DplusMatchConfig.class);
        Assert.assertNotNull(deserialized);
        // base rule
        DplusMatchRule baseRule2 = deserialized.getBaseRule();
        Assert.assertNotNull(baseRule2);
        Assert.assertEquals(baseRule2.getExclusionCriteria().size(), 1);
        Assert.assertEquals(new ArrayList<>(baseRule2.getExclusionCriteria()).get(0), OutOfBusiness);

        Assert.assertEquals(deserialized.getSpecialRules().size(), 1);
        DplusMatchConfig.SpeicalRule specialRule = deserialized.getSpecialRules().get(0);
        Assert.assertEquals(specialRule.getMatchKey(), Country);
    }

}
