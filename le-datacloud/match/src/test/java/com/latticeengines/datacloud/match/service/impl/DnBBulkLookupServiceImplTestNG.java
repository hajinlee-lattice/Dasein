package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DnBBulkLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupServiceImplTestNG.class);

    @Inject
    private DnBBulkLookupDispatcherImpl dnbBulkLookupDispatcher;

    @Inject
    private DnBBulkLookupStatusCheckerImpl dnbBulkLookupStatusChecker;

    @Inject
    private DnBBulkLookupFetcherImpl dnbBulkLookupFetcher;
    
    @Inject
    private DnBAuthenticationService dnbAuthenticationService;

    private static final String INVALID_BATCH_TOKEN = "wExXYdjH60CGv4kfWP3oXxVkIdxUla2LBy4rFzBKHV3Gb5LCqOM8sGxdArHYq"
            + "GXlgnBOdaUcDLho1qzVAYk4fS/oBYwrmAjEX6W4pV+i2jzd6Pi9VBruiLsAbF9rjvRZ5OxlpPQRcrR7B3Di9kIc5XheB1uax7VeeTv"
            + "fqWDHl1KHRQ6+KlT9n1oPWrFjj4f7Mre3CndoQU28K/4V8WuqfKwT6tqa7sPQ/Yb4n4aVhd5fsk+LiOYxgUqZQbOvgCbu424m8sTdX"
            + "Ph3Ehf0p/DnlU/Jb3LjE9HzC3JuUd16fVf6Zdw9Z9NTbgswIb2Y+MV1bqI7Jdj8L0mg9lffON7rAAe4IvZjhzFYWoci6gID4pszAws"
            + "BcUcHT/aq9LFhU/cexe4EHeYBgj3B6zIvjEMJrg==";

    @Test(groups = "dnb", enabled = true, priority = 1, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testDnBBulkLookup() {
        DnBBatchMatchContext batchContext = dnbBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.SUBMITTED);

        List<DnBBatchMatchContext> contexts = Arrays.asList(batchContext);
        dnbBulkLookupStatusChecker.checkStatus(contexts);
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
            dnbBulkLookupStatusChecker.checkStatus(contexts);
        }

        batchContext = dnbBulkLookupFetcher.getResult(batchContext);
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

    // Test cases:
    // 1. Records with errors
    // 2. DnB token cached is expired -- should refresh token automatically
    @Test(groups = "dnb", enabled = true, priority = 2, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testDnBBulkLookupWithErrorRecords() {
        // Set token to be invalid
        dnbAuthenticationService.refreshToken(DnBKeyType.BATCH, INVALID_BATCH_TOKEN);
        // Wait for local cache to be refreshed
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
        }
        // Expected dispatch service to refresh token and make a successful call
        // via
        // retry
        DnBBatchMatchContext batchContext = dnbBulkLookupDispatcher.sendRequest(generateInputWithErrorRecords());
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.SUBMITTED);

        // Set token to be invalid
        dnbAuthenticationService.refreshToken(DnBKeyType.BATCH, INVALID_BATCH_TOKEN);
        // Wait for local cache to be refreshed
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
        }
        // Expected status service to refresh token and make a successful call
        // via retry
        List<DnBBatchMatchContext> contexts = Arrays.asList(batchContext);
        dnbBulkLookupStatusChecker.checkStatus(contexts);
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
            dnbBulkLookupStatusChecker.checkStatus(contexts);
        }

        // Set token to be invalid
        dnbAuthenticationService.refreshToken(DnBKeyType.BATCH, INVALID_BATCH_TOKEN);
        // Wait for local cache to be refreshed
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
        }
        // Expected fetch service to refresh token and make a successful call
        // via retry
        batchContext = dnbBulkLookupFetcher.getResult(batchContext);
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

    /**
     * 1. Notice that DnB is changing error response schema periodically without
     * notification. Should update the error response schema in the test up to
     * date if there is any change detected. (TODO: Better solution should be
     * inject invalid token in the service, then call DnB API to get latest
     * error response, which will catch the issue early.)
     *
     * 2. 3 bulk API returns different error response schema. Don't merge 3 test
     * cases into one.
     */
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testParseDnBHttpError() {
        Assert.assertEquals(dnbBulkLookupDispatcher.parseDnBHttpError(
                new HttpClientErrorException(HttpStatus.UNAUTHORIZED, HttpStatus.UNAUTHORIZED.name(),
                        ("<bat:ProcessBatchResponse ServiceVersionNumber=\"2.0\" xmlns:bat=\"http://services.dnb.com/BatchServiceV1.0\">"
                        + "<BatchDetail><ApplicationBatchID>Id-8795ef5d134a89e2e2038b40</ApplicationBatchID>"
                        + "<ServiceBatchID/></BatchDetail>" //
                        + "<BatchResult><SeverityText>Error</SeverityText><ResultID>SC001</ResultID>"
                        + "<ResultText>User Credentials Missing. Please fill in the user credentials and try again.</ResultText>"
                        + "<ResultMessage>"
                        + "<ResultDescription>User Credentials or Token or Application Id Missing. Please fill in the user credentials and try again.</ResultDescription>"
                        + "</ResultMessage></BatchResult></bat:ProcessBatchResponse>").getBytes(), null)), DnBReturnCode.UNAUTHORIZED);

        Assert.assertEquals(
                dnbBulkLookupStatusChecker.parseDnBHttpError(
                        new HttpClientErrorException(HttpStatus.UNAUTHORIZED, HttpStatus.UNAUTHORIZED.name(),
                                ("<TransactionResult xmlns=\"http://services.dnb.com/TransactionFaultV2.0\">"
                                + "<SeverityText>Error</SeverityText><ResultCode>SC001</ResultCode>"
                                + "<ResultText>User Credentials Missing. Please fill in the user credentials and try again.</ResultText>"
                                + "<ResultMessage>"
                                + "<ResultDescription>User Credentials or Token or Application Id Missing. Please fill in the user credentials and try again.</ResultDescription>"
                                + "</ResultMessage></TransactionResult>").getBytes(), null)),
                DnBReturnCode.UNAUTHORIZED);

        Assert.assertEquals(
                dnbBulkLookupFetcher.parseDnBHttpError(
                        new HttpClientErrorException(HttpStatus.UNAUTHORIZED, HttpStatus.UNAUTHORIZED.name(),
                                ("<bat:GetBatchResultsResponse ServiceVersionNumber=\"2.0\" xmlns:bat=\"http://services.dnb.com/BatchServiceV1.0\">"
                                + "<TransactionResult><SeverityText>Error</SeverityText>"
                                + "<ResultID>SC001</ResultID>"
                                + "<ResultText>User Credentials Missing. Please fill in the user credentials and try again.</ResultText>"
                                + "<ResultMessage>"
                                + "<ResultDescription>User Credentials or Token or Application Id Missing. Please fill in the user credentials and try again.</ResultDescription>"
                                        + "</ResultMessage></TransactionResult></bat:GetBatchResultsResponse>")
                                                .getBytes(),
                                null)),
                DnBReturnCode.UNAUTHORIZED);
    }

    public static Object[][] getEntityInputData() {
        return new Object[][] {
                { "AMAZON", "CHICAGO", "ILLINOIS", "US", "002078755", 7, new DnBMatchGrade("AZZAAZZZFFZ"),
                        "AMAZON.COM, INC.", "2801 S WESTERN AVE", "CHICAGO", "IL", "US", "606085213",
                        "(773) 869-9056" },
                { "GOOGLE GERMANY", "HAMBURG", null, "DE", "330465266", 7, new DnBMatchGrade("AZZAZZZZZFZ"),
                        "Google Germany GmbH", "ABC-Str. 19", "Hamburg", "DE", "DE", "20354", "04049219077" },
                { "GORMAN MFG CO INC", "SACRAMENTO", "CA", "US", "009175688", 7, new DnBMatchGrade("AZZAAZZZFFZ"),
                        "GORMAN MANUFACTURING COMPANY, INC.", "8129 JUNIPERO ST SUITE A", "SACRAMENTO", "CA", "US",
                        "958281603", "(530) 662-0211" }
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
