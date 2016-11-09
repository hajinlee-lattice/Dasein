package com.latticeengines.datacloud.match.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;

public class DnBBulkLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImplTestNG.class);

    @Autowired
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnBBulkLookupFetcher;

    @Test(groups = "functional", enabled = true)
    public void testBulkLookupService() {
        DnBBulkMatchInfo info = dnBBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(info.getDnbCode(), DnBReturnCode.OK);

        dnBBulkLookupFetcher.getResult(info);
    }

    @Test(groups = "functional", enabled = true)
    public void testBulkLookupFetcher() {
        DnBBulkMatchInfo info = new DnBBulkMatchInfo();

        info.setTimestamp("2016-11-09T07:45:05-05:00");
        info.setServiceBatchId("2215928E1");

        List<DnBMatchOutput> output = dnBBulkLookupFetcher.getResult(info);
        Assert.assertEquals(output.size(), 4);
    }

    static String[][] input = { { "Benchmark Blinds", "Gilbert", "Arizona", "US" },
            { "El Camino Machine Welding LLC", "Salinas", "California", "US" },
            { "Cornerstone Alliance Church", "Canon City", "Colorado", "US" },
            { "  Gorman Manufacturing  ", "", "", "  US  " } };

    Map<String, MatchKeyTuple> generateInput() {
        Map<String, MatchKeyTuple> tuples = new HashMap<String, MatchKeyTuple>();
        for (String[] record : input) {
            String uuid = UUID.randomUUID().toString();
            MatchKeyTuple tuple = new MatchKeyTuple();
            tuple.setName(record[0]);
            tuple.setCity(record[1]);
            tuple.setState(record[2]);
            tuple.setCountry(record[3]);
            tuples.put(uuid, tuple);
        }
        return tuples;
    }
}
