package com.latticeengines.domain.exposed.propdata.manage;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class MatchStatisticsUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() throws IOException {
        MatchStatistics statistics = new MatchStatistics();
        statistics.setTimeElapsedInMsec(1234L);

        String serializedString = JsonUtils.serialize(statistics);

        MatchStatistics deserialized = JsonUtils.deserialize(serializedString, MatchStatistics.class);
        Assert.assertEquals(deserialized.getTimeElapsedInMsec(), new Long(1234L));
    }

}
