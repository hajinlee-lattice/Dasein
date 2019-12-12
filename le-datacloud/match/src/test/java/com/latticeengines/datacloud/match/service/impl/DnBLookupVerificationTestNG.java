package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.common.exposed.util.NameStringStandardizationUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

// Use Fortune1000 to compare match result between DnB realtime match and DnB bulk match
public class DnBLookupVerificationTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DnBLookupVerificationTestNG.class);

    @Inject
    private CountryCodeService countryCodeService;

    @Inject
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    @Inject
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Inject
    private DnBBulkLookupStatusChecker dnbBulkLookupStatusChecker;

    @Inject
    private DnBBulkLookupFetcher dnBBulkLookupFetcher;

    @SuppressWarnings("unused")
    private static final String FORTUNE1000_SMALL_FILENAME = "Fortune1000_Small.csv";

    private static final String FORTUNE1000_FILENAME = "Fortune1000.csv";

    private static final String[] FORTUNE1000_FILENAME_HEADER = { "﻿Name", "Domain", "City", "State", "Country" };

    @SuppressWarnings("unused")
    private static final String FORTUNE1000_NAME = "Name";

    private static final String FORTUNE1000_CITY = "City";

    private static final String FORTUNE1000_STATE = "State";

    private static final String FORTUNE1000_COUNTRY = "Country";

    private Map<String, DnBMatchContext> contextsRealtime = new HashMap<String, DnBMatchContext>();

    private Map<String, DnBMatchContext> contextsBulk = new HashMap<String, DnBMatchContext>();

    private CSVParser csvFileParser;

    @Test(groups = "dnb", enabled = false)
    public void testConsistency() {
        // prepareFortune1000InputData(FORTUNE1000_SMALL_FILENAME, true, true);
        prepareFortune1000InputData(FORTUNE1000_FILENAME, true, true);
        // Submit to DnB bulk match
        DnBBatchMatchContext batchContext = prepareBulkMatchInput();
        batchContext = dnBBulkLookupDispatcher.sendRequest(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.SUBMITTED);
        log.info(String.format("serviceBatchId=%s", batchContext.getServiceBatchId()));
        // Submit to DnB realtime match
        realtimeLookup();
        // Get result from DnB bulk match
        batchContext = bulkLookup(batchContext);
        // Compare results
        compareResults();
    }

    private void realtimeLookup() {
        for (String lookupRequestId : contextsRealtime.keySet()) {
            DnBMatchContext context = contextsRealtime.get(lookupRequestId);
            DnBMatchContext res = dnBRealTimeLookupService.realtimeEntityLookup(context);
            context.copyMatchResult(res);
            log.info(String.format(
                    "Realtime match result for request %s: Status=%s, Duns=%s, ConfidenceCode=%d, MatchGrade=%s",
                    context.getLookupRequestId(), res.getDnbCode() == null ? "null" : res.getDnbCode(),
                    context.getDuns(), context.getConfidenceCode(),
                    context.getMatchGrade() == null ? "null" : context.getMatchGrade().getRawCode()));
        }
    }

    private DnBBatchMatchContext bulkLookup(DnBBatchMatchContext batchContext) {
        List<DnBBatchMatchContext> contexts = Arrays.asList(batchContext);
        dnbBulkLookupStatusChecker.checkStatus(contexts);

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
            dnbBulkLookupStatusChecker.checkStatus(contexts);
        }
        batchContext = dnBBulkLookupFetcher.getResult(batchContext);
        Assert.assertEquals(batchContext.getDnbCode(), DnBReturnCode.OK);
        return batchContext;
    }

    private void compareResults() {
        int inconsistentResults = 0;
        Assert.assertEquals(contextsRealtime.size(), contextsBulk.size());
        for (String lookupRequestId : contextsRealtime.keySet()) {
            DnBMatchContext contextFromRealtime = contextsRealtime.get(lookupRequestId);
            DnBMatchContext contextFromBatch = contextsBulk.get(lookupRequestId);
            Assert.assertNotNull(contextFromRealtime);
            Assert.assertNotNull(contextFromBatch);
            if (!compareResult(contextFromRealtime, contextFromBatch)) {
                inconsistentResults++;
            }
        }
        Assert.assertEquals(inconsistentResults, 0);
    }

    private boolean compareResult(DnBMatchContext contextFromRealtime, DnBMatchContext contextFromBatch) {
        if (contextFromRealtime.getDnbCode().equals(DnBReturnCode.OK)
                && contextFromBatch.getDnbCode().equals(DnBReturnCode.OK)
                && contextFromRealtime.getDuns().equals(contextFromBatch.getDuns())
                && contextFromRealtime.getConfidenceCode().equals(contextFromBatch.getConfidenceCode())
                && contextFromRealtime.getMatchGrade().equals(contextFromBatch.getMatchGrade())) {
            return true;
        }
        if (contextFromRealtime.getDnbCode().equals(DnBReturnCode.DISCARD)
                && contextFromBatch.getDnbCode().equals(DnBReturnCode.DISCARD)
                && contextFromRealtime.getConfidenceCode().equals(contextFromBatch.getConfidenceCode())
                && contextFromRealtime.getMatchGrade().equals(contextFromBatch.getMatchGrade())) {
            return true;
        }
        if (contextFromRealtime.getDnbCode().equals(DnBReturnCode.UNMATCH)
                && contextFromBatch.getDnbCode().equals(DnBReturnCode.UNMATCH)) {
            return true;
        }
        log.info("-------------------------------------");
        log.info(String.format("Name: %s, CountryCode: %s, StateCode: %s, City: %s",
                contextFromBatch.getInputNameLocation().getName(),
                contextFromBatch.getInputNameLocation().getCountryCode(),
                LocationUtils.getStardardStateCode(contextFromBatch.getInputNameLocation().getCountry(),
                        contextFromBatch.getInputNameLocation().getState()),
                contextFromBatch.getInputNameLocation().getCity()));
        log.info(String.format("Duns: %s(realtime) %s(bulk)", contextFromRealtime.getDuns(),
                contextFromBatch.getDuns()));
        log.info(String.format("DnBReturnCode: %s(realtime) %s(bulk)",
                contextFromRealtime.getDnbCode() == null ? "null" : contextFromRealtime.getDnbCode(),
                contextFromBatch.getDnbCode() == null ? "null" : contextFromBatch.getDnbCode()));
        log.info(String.format("ConfidenceCode: %s(realtime) %s(bulk)", contextFromRealtime.getConfidenceCode(),
                contextFromBatch.getConfidenceCode()));
        log.info(String.format("MatchGrade: %s(realtime) %s(bulk)",
                contextFromRealtime.getMatchGrade() == null ? "null" : contextFromRealtime.getMatchGrade().getRawCode(),
                contextFromBatch.getMatchGrade() == null ? "null" : contextFromBatch.getMatchGrade().getRawCode()));
        return false;
    }

    private DnBBatchMatchContext prepareBulkMatchInput() {
        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
        batchContext.setContexts(contextsBulk);
        return batchContext;
    }

    private void prepareFortune1000InputData(String fileName, boolean includeState, boolean includeCity) {
        try {
            contextsRealtime = new HashMap<>();
            contextsBulk = new HashMap<>();
            InputStream fileStream = ClassLoader.getSystemResourceAsStream("matchinput/" + fileName);
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FORTUNE1000_FILENAME_HEADER)
                    .withRecordSeparator("\n");
            csvFileParser = new CSVParser(new InputStreamReader(fileStream), csvFileFormat);
            List<CSVRecord> csvRecords = csvFileParser.getRecords();
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);
                String country = record.get(FORTUNE1000_COUNTRY);
                String countryCode = countryCodeService.getCountryCode(country);
                country = countryCodeService.getStandardCountry(country);
                String name = record.get(0);
                name = NameStringStandardizationUtils.getStandardString(name);
                String state = null;
                if (includeState) {
                    state = record.get(FORTUNE1000_STATE);
                    state = LocationUtils.getStandardState(country, state);
                }
                String city = null;
                if (includeCity) {
                    city = record.get(FORTUNE1000_CITY);
                    city = NameStringStandardizationUtils.getStandardString(city);
                }
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
            csvFileParser.close();
            log.info(String.format("Submitted %d rows from Fortune1000 to DnB api", contextsRealtime.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
