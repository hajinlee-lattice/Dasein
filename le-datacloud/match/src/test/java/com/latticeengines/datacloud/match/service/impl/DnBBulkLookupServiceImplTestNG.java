package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class DnBBulkLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBBulkLookupServiceImplTestNG.class);

    @Autowired
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnBBulkLookupFetcher;
    

    @Test(groups = "functional", enabled = false)
    public void testBulkLookupService() {
        DnBBatchMatchContext batchContext = dnBBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
    }


    @Test(groups = "functional", enabled = true)
    public void testBulkLookupFetcher() {
        DnBBatchMatchContext batchContext = generateInput();

        batchContext.setTimestamp(new Date());
        batchContext.setServiceBatchId("2239595E1");

        Map<String, DnBMatchContext> output = dnBBulkLookupFetcher.getResult(batchContext).getContexts();
        Assert.assertEquals(output.size(), getEntityInputData().length);
        for (String lookupRequestId : output.keySet()) {
            DnBMatchContext result = output.get(lookupRequestId);
            log.info("Request " + result.getLookupRequestId() + ": " + result.getDuns());
        }
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
        dnBBulkLookupFetcher.getResult(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.RATE_LIMITING);
        try {
            Thread.sleep(70000);
        } catch (InterruptedException e) {
        }
        dnBBulkLookupFetcher.getResult(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);

    }

    public static Object[][] getEntityInputData() {
        return new Object[][] {
                //{ "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.OK, "038796548", 8,new DnBMatchGrade("AZZAAZZZFAB") },
                //{ "Désirée Daude", null, null, "DE", DnBReturnCode.BAD_REQUEST, null, null, null },
                //{ "ABCDEFG", "New York", "Washinton", "US", DnBReturnCode.UNMATCH, null, null, null },
                //{ "Gorman Manufacturing", null, null, "US", DnBReturnCode.DISCARD, null, 6, new DnBMatchGrade("AZZZZZZZFZZ") },
            {"Amazon Inc", "Chicago", "Illinois", "US"}
                };
    }

    private DnBBatchMatchContext generateInput() {
        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
        Map<String, DnBMatchContext> contexts = new HashMap<String, DnBMatchContext>();
        for (int i = 0; i < getEntityInputData().length; i++) {
            Object[] record = getEntityInputData()[i];
            String uuid = String.valueOf(i);
            MatchKeyTuple tuple = new MatchKeyTuple();
            tuple.setName((String) record[0]);
            tuple.setCity((String) record[1]);
            tuple.setState((String) record[2]);
            tuple.setCountryCode((String) record[3]);
            DnBMatchContext context = new DnBMatchContext();
            context.setLookupRequestId(uuid);
            context.setInputNameLocation(tuple);
            contexts.put(uuid, context);
        }
        batchContext.setContexts(contexts);
        return batchContext;
    }

}
