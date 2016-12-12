package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

// Test 1: use Fortune1000 to compare match result between DnB realtime match and DnB bulk match
// Test 2: submit DnB bulk match with large input set
public class DnBLookupVerificationTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(DnBLookupVerificationTestNG.class);

    @Autowired
    private CountryCodeService countryCodeService;

    @Autowired
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnBBulkLookupFetcher;

    private static final String FORTUNE1000_SMALL_FILENAME = "Fortune1000_Small.csv";

    private static final String FORTUNE1000_FILENAME = "Fortune1000.csv";

    private static final String[] FORTUNE1000_FILENAME_HEADER = { "﻿Name", "Domain", "City", "State", "Country" };

    private static final String FORTUNE1000_NAME = "Name";

    private static final String FORTUNE1000_CITY = "City";

    private static final String FORTUNE1000_STATE = "State";

    private static final String FORTUNE1000_COUNTRY = "Country";

    private Map<String, DnBMatchContext> contextsRealtime = new HashMap<String, DnBMatchContext>();

    private Map<String, DnBMatchContext> contextsBulk = new HashMap<String, DnBMatchContext>();

    @Test(groups = "dnb", enabled = true)
    public void testConsistency() {
        prepareFortune1000SmallInputData();
        // Submit to DnB bulk match
        DnBBatchMatchContext batchContext = prepareBulkMatchInput();
        batchContext = dnBBulkLookupDispatcher.sendRequest(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
        log.info(String.format("serviceBatchId=%s", batchContext.getServiceBatchId()));
        // Submit to DnB realtime match
        realtimeLookup();
        // Get result from DnB bulk match
        batchContext = bulkLookup(batchContext);
        // Compare results
        compareResults();
    }

    @Test(groups = "dnb", enabled = true, dependsOnMethods = { "testConsistency" })
    public void testLargeBulkMatch() {
        prepareFortune1000LargeInputData();
        // Submit to DnB bulk match
        DnBBatchMatchContext batchContext = prepareBulkMatchInput();
        batchContext = dnBBulkLookupDispatcher.sendRequest(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
        log.info(String.format("serviceBatchId=%s", batchContext.getServiceBatchId()));
        batchContext = bulkLookup(batchContext);
        verifyBulkMatchResult(batchContext);
    }

    private void realtimeLookup() {
        for (String lookupRequestId : contextsRealtime.keySet()) {
            DnBMatchContext context = contextsRealtime.get(lookupRequestId);
            DnBMatchContext res = dnBRealTimeLookupService.realtimeEntityLookup(context);
            context.copyMatchResult(res);
            log.info(String.format(
                    "Realtime match result for request %s: Status = %s, Duns = %s, ConfidenceCode = %d, MatchGrade = %s",
                    context.getLookupRequestId(), res.getDnbCode() == null ? "null" : res.getDnbCode().getMessage(),
                    context.getDuns(), context.getConfidenceCode(),
                    context.getMatchGrade() == null ? "null" : context.getMatchGrade().getRawCode()));
        }
    }

    private DnBBatchMatchContext bulkLookup(DnBBatchMatchContext batchContext) {
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
        return batchContext;
    }

    private void compareResults() {
        Assert.assertEquals(contextsRealtime.size(), contextsBulk.size());
        for (String lookupRequestId : contextsRealtime.keySet()) {
            DnBMatchContext contextFromRealtime = contextsRealtime.get(lookupRequestId);
            DnBMatchContext contextFromBatch = contextsBulk.get(lookupRequestId);
            Assert.assertNotNull(contextFromRealtime);
            Assert.assertNotNull(contextFromBatch);
            compareResult(contextFromRealtime, contextFromBatch);
        }
    }

    private void verifyBulkMatchResult(DnBBatchMatchContext batchContext) {
        int rowMatched = 0;
        Map<String, DnBMatchContext> contexts = batchContext.getContexts();
        for (String lookupRequestId : contexts.keySet()) {
            DnBMatchContext context = contexts.get(lookupRequestId);
            if (context.getDuns() != null) {
                rowMatched++;
            }
        }
        log.info(String.format("%d rows out of %d got matched", rowMatched, batchContext.getContexts().size()));
    }

    private void compareResult(DnBMatchContext contextFromRealtime, DnBMatchContext contextFromBatch) {
        log.info("-------------------------------------");
        log.info(String.format("Name: %s, CountryCode: %s, State: %s, City: %s",
                contextFromBatch.getInputNameLocation().getName(),
                contextFromBatch.getInputNameLocation().getCountryCode(),
                contextFromBatch.getInputNameLocation().getState(), contextFromBatch.getInputNameLocation().getCity()));
        log.info(String.format("Duns: %s(realtime) %s(bulk)", contextFromRealtime.getDuns(),
                contextFromBatch.getDuns()));
        log.info(String.format("DnBReturnCode: %s(realtime) %s(bulk)",
                contextFromRealtime.getDnbCode() == null ? "null" : contextFromRealtime.getDnbCode().getMessage(),
                contextFromBatch.getDnbCode() == null ? "null" : contextFromBatch.getDnbCode().getMessage()));
        log.info(String.format("ConfidenceCode: %s(realtime) %s(bulk)", contextFromRealtime.getConfidenceCode(),
                contextFromBatch.getConfidenceCode()));
        log.info(String.format("MatchGrade: %s(realtime) %s(bulk)",
                contextFromRealtime.getMatchGrade() == null ? "null" : contextFromRealtime.getMatchGrade().getRawCode(),
                contextFromBatch.getMatchGrade() == null ? "null" : contextFromBatch.getMatchGrade().getRawCode()));
        /*
        Assert.assertEquals(contextFromRealtime.getDuns(), contextFromBatch.getDuns());
        Assert.assertEquals(contextFromRealtime.getDnbCode(), contextFromBatch.getDnbCode());
        Assert.assertEquals(contextFromRealtime.getConfidenceCode(), contextFromBatch.getConfidenceCode());
        Assert.assertEquals(contextFromRealtime.getMatchGrade(), contextFromBatch.getMatchGrade());
        */
    }

    private DnBBatchMatchContext prepareBulkMatchInput() {
        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
        batchContext.setContexts(contextsBulk);
        return batchContext;
    }

    private void prepareFortune1000SmallInputData() {
        try {
            contextsRealtime = new HashMap<String, DnBMatchContext>();
            contextsBulk = new HashMap<String, DnBMatchContext>();
            InputStream fileStream = ClassLoader.getSystemResourceAsStream("matchinput/" + FORTUNE1000_SMALL_FILENAME);
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FORTUNE1000_FILENAME_HEADER)
                    .withRecordSeparator("\n");
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(fileStream), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);
                String country = record.get(FORTUNE1000_COUNTRY);
                country = LocationUtils.getStandardCountry(country);
                String countryCode = countryCodeService.getCountryCode(country);
                String name = record.get(0);
                name = StringUtils.getStandardString(name);
                String state = record.get(FORTUNE1000_STATE);
                state = LocationUtils.getStandardState(country, state);
                String city = record.get(FORTUNE1000_CITY);
                city = StringUtils.getStandardString(city);
                if (name != null && countryCode != null) {
                    MatchKeyTuple input = new MatchKeyTuple();
                    input.setCountry(country);
                    input.setCountryCode(countryCode);
                    input.setName(name);
                    input.setState(state);
                    input.setCity(city);
                    String lookupRequestId = UUID.randomUUID().toString();
                    DnBMatchContext contextRealtime = new DnBMatchContext();
                    contextRealtime.setInputNameLocation(input);
                    contextRealtime.setLookupRequestId(lookupRequestId);
                    contextsRealtime.put(contextRealtime.getLookupRequestId(), contextRealtime);
                    DnBMatchContext contextBulk = new DnBMatchContext();
                    contextBulk.setInputNameLocation(input);
                    contextBulk.setLookupRequestId(lookupRequestId);
                    contextsBulk.put(contextBulk.getLookupRequestId(), contextBulk);
                }
            }
            log.info(String.format("Submitted %d rows from Fortune1000 to DnB api", contextsRealtime.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareFortune1000LargeInputData() {
        try {
            contextsBulk = new HashMap<String, DnBMatchContext>();
            InputStream fileStream = ClassLoader.getSystemResourceAsStream("matchinput/" + FORTUNE1000_FILENAME);
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FORTUNE1000_FILENAME_HEADER)
                    .withRecordSeparator("\n");
            CSVParser csvFileParser = new CSVParser(new InputStreamReader(fileStream), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int j = 0; j < 10; j++) {
                for (int i = 1; i < csvRecords.size(); i++) {
                    CSVRecord record = csvRecords.get(i);
                    String country = record.get(FORTUNE1000_COUNTRY);
                    country = LocationUtils.getStandardCountry(country);
                    String countryCode = countryCodeService.getCountryCode(country);
                    String name = record.get(0);
                    name = StringUtils.getStandardString(name);
                    String state = record.get(FORTUNE1000_STATE);
                    state = LocationUtils.getStandardState(country, state);
                    String city = record.get(FORTUNE1000_CITY);
                    city = StringUtils.getStandardString(city);
                    if (name != null && countryCode != null) {
                        MatchKeyTuple input = new MatchKeyTuple();
                        input.setCountry(country);
                        input.setCountryCode(countryCode);
                        input.setName(name);
                        input.setState(state);
                        input.setCity(city);
                        DnBMatchContext contextBulk = new DnBMatchContext();
                        contextBulk.setInputNameLocation(input);
                        contextBulk.setLookupRequestId(UUID.randomUUID().toString());
                        contextsBulk.put(contextBulk.getLookupRequestId(), contextBulk);
                    }
                }
            }
            log.info(String.format("Submitted %d rows from Fortune1000 to DnB", contextsBulk.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
