package com.latticeengines.datacloud.match.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class DnBBulkLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupServiceImplTestNG.class);

    @Autowired
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnBBulkLookupFetcher;
    
    @Test(groups = "dnb", enabled = true)
    public void testDnBBulkLookup() {
        DnBBatchMatchContext batchContext = dnBBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.SUBMITTED);

        batchContext = dnBBulkLookupFetcher.getResult(batchContext);
        while (batchContext.getDnbCode() == DnBReturnCode.IN_PROGRESS
                || batchContext.getDnbCode() == DnBReturnCode.RATE_LIMITING) {
            if (batchContext.getTimestamp() == null
                    || (System.currentTimeMillis() - batchContext.getTimestamp().getTime()) / 1000 / 60 > 60) {
                break;
            }
            try {
                Thread.sleep(60000);
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
            Assert.assertEquals(result.getDuns(), getEntityInputData()[Integer.valueOf(lookupRequestId)][4]);
            Assert.assertNotNull(result.getDuration());
            Assert.assertEquals((int) result.getConfidenceCode(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][5]);
            Assert.assertEquals(result.getMatchGrade(), getEntityInputData()[Integer.valueOf(lookupRequestId)][6]);
            log.info(String.format(
                    "Name = %s, Street = %s, City = %s, State = %s, CountryCode = %s, ZipCode = %s, PhoneNumber = %s, OutOfBusiness = %b",
                    result.getMatchedNameLocation().getName(), result.getMatchedNameLocation().getStreet(),
                    result.getMatchedNameLocation().getCity(), result.getMatchedNameLocation().getState(),
                    result.getMatchedNameLocation().getCountryCode(), result.getMatchedNameLocation().getZipcode(),
                    result.getMatchedNameLocation().getPhoneNumber(), result.isOutOfBusiness()));
            Assert.assertEquals(result.getMatchedNameLocation().getName(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][7]);
            Assert.assertEquals(result.getMatchedNameLocation().getStreet(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][8]);
            Assert.assertEquals(result.getMatchedNameLocation().getCity(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][9]);
            Assert.assertEquals(result.getMatchedNameLocation().getState(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][10]);
            Assert.assertEquals(result.getMatchedNameLocation().getCountryCode(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][11]);
            Assert.assertEquals(result.getMatchedNameLocation().getZipcode(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][12]);
            Assert.assertEquals(result.getMatchedNameLocation().getPhoneNumber(),
                    getEntityInputData()[Integer.valueOf(lookupRequestId)][13]);
        }
    }

    @Test(groups = "dnb", enabled = true)
    public void testDnBBulkLookupWithErrorRecords() {
        DnBBatchMatchContext batchContext = dnBBulkLookupDispatcher.sendRequest(generateInputWithErrorRecords());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.SUBMITTED);

        batchContext = dnBBulkLookupFetcher.getResult(batchContext);
        while (batchContext.getDnbCode() == DnBReturnCode.IN_PROGRESS
                || batchContext.getDnbCode() == DnBReturnCode.RATE_LIMITING) {
            if (batchContext.getTimestamp() == null
                    || (System.currentTimeMillis() - batchContext.getTimestamp().getTime()) / 1000 / 60 > 60) {
                break;
            }
            try {
                Thread.sleep(60000);
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
            log.info(String.format("Request %s: DnBReturnCode = %s, DUNS = %s", result.getLookupRequestId(),
                    result.getDnbCodeAsString(), result.getDuns()));
            Assert.assertEquals(result.getDnbCode(),
                    getEntityInputDataWithErrorRecords()[Integer.valueOf(lookupRequestId)][4]);
        }
    }

    public static Object[][] getEntityInputData() {
        return new Object[][] {
                { "AMAZON INC", "CHICAGO", "ILLINOIS", "US", "002078755", 7, new DnBMatchGrade("AZZAAZZZFFZ"),
                        "AMAZON", "2801 S WESTERN AVE", "CHICAGO", "IL", "US", "606085220", "(773) 869-9056" },
                { "GOOGLE GERMANY", "HAMBURG", null, "DE", "330465266", 7, new DnBMatchGrade("AZZAZZZZZFZ"),
                        "Google Germany GmbH", "ABC-Str. 19", "Hamburg", "DE", "DE", "20354", "040808179000" },
                { "GORMAN MFG CO INC", "SACRAMENTO", "CA", "US", "009175688", 7, new DnBMatchGrade("AZZAAZZZFFZ"),
                        "GORMAN MFG CO INC", "8129 JUNIPERO ST STE A", "SACRAMENTO", "CA", "US", "958281603",
                        "(530) 662-0211" }
                };
    }

    public static Object[][] getEntityInputDataWithErrorRecords() {
        return new Object[][] { { "AMAZON INC", "CHICAGO", "ILLINOIS", "US", DnBReturnCode.OK },
                { "GOOGLE", null, null, null, DnBReturnCode.UNMATCH },
                { "GOOGLE", null, null, "ZZ", DnBReturnCode.UNMATCH }, };
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

    private DnBBatchMatchContext generateInputWithErrorRecords() {
        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
        Map<String, DnBMatchContext> contexts = new HashMap<String, DnBMatchContext>();
        for (int i = 0; i < getEntityInputDataWithErrorRecords().length; i++) {
            Object[] record = getEntityInputDataWithErrorRecords()[i];
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
