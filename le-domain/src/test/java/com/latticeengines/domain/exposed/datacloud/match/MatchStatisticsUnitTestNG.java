package com.latticeengines.domain.exposed.datacloud.match;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class MatchStatisticsUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {
        MatchStatistics statistics = new MatchStatistics();
        statistics.setTimeElapsedInMsec(1234L);

        String serializedString = JsonUtils.serialize(statistics);

        MatchStatistics deserialized = JsonUtils.deserialize(serializedString, MatchStatistics.class);
        Assert.assertEquals(deserialized.getTimeElapsedInMsec(), Long.valueOf(1234L));
    }

}
