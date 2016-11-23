package com.latticeengines.datacloud.match.service.impl;

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
    
    @Test(groups = "dnb", enabled = true)
    public void testDnBBulkLookup() {
        DnBBatchMatchContext batchContext = dnBBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);

        batchContext = dnBBulkLookupFetcher.getResult(batchContext);
        while (batchContext.getDnbCode() == DnBReturnCode.IN_PROGRESS
                || batchContext.getDnbCode() == DnBReturnCode.RATE_LIMITING) {
            if (batchContext.getTimestamp() == null
                    || (System.currentTimeMillis() - batchContext.getTimestamp().getTime()) / 1000 / 60 > 60) {
                break;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            batchContext = dnBBulkLookupFetcher.getResult(batchContext);
        }
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
        Map<String, DnBMatchContext> output = batchContext.getContexts();
        Assert.assertEquals(output.size(), getEntityInputData().length);
        for (String lookupRequestId : output.keySet()) {
            DnBMatchContext result = output.get(lookupRequestId);
            log.info(String.format("Request %s: duns = %s, duration = %d, confidence code = %d, match grade = %s",
                    result.getLookupRequestId(), result.getDuns(), result.getDuration(), result.getConfidenceCode(),
                    result.getMatchGrade().getRawCode()));
            Assert.assertEquals(result.getDuns(), "013919572");
            Assert.assertNotNull(result.getDuration());
            Assert.assertEquals((int) result.getConfidenceCode(), 7);
            Assert.assertEquals(result.getMatchGrade().getRawCode(), "AZZAAZZZFFZ");
        }
    }

    public static Object[][] getEntityInputData() {
        return new Object[][] {
                //{ "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.OK, "038796548", 8,new DnBMatchGrade("AZZAAZZZFAB") },
                //{ "Désirée Daude", null, null, "DE", DnBReturnCode.BAD_REQUEST, null, null, null },
                //{ "ABCDEFG", "New York", "Washinton", "US", DnBReturnCode.UNMATCH, null, null, null },
                //{ "Gorman Manufacturing", null, null, "US", DnBReturnCode.DISCARD, null, 6, new DnBMatchGrade("AZZZZZZZFZZ") },
                { "AMAZON INC", "CHICAGO", "ILLINOIS", "US" }
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
