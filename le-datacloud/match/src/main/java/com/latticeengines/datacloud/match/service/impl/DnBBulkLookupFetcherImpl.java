package com.latticeengines.datacloud.match.service.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@Component("dnbBulkLookupFetcher")
public class DnBBulkLookupFetcherImpl extends BaseDnBBulkLookupFetcherImpl implements DnBBulkLookupFetcher {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupFetcherImpl.class);

    @Inject
    private DnBBulkLookupFetcherImpl _self;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.bulk.output.content.object.xpath}")
    private String contentObjectXpath;

    @Value("${datacloud.dnb.bulk.result.id.xpath}")
    private String batchResultIdXpath;

    @Value("${datacloud.dnb.bulk.receive.timestamp.xpath}")
    private String receiveTimestampXpath;

    @Value("${datacloud.dnb.bulk.complete.timestamp.xpath}")
    private String completeTimestampXpath;

    @Value("${datacloud.dnb.bulk.office.id}")
    private int officeID;

    @Value("${datacloud.dnb.bulk.service.number}")
    private int serviceNumber;

    @Value("${datacloud.dnb.bulk.getresult.url.format}")
    private String getResultUrlFormat;

    @Value("${datacloud.dnb.bulk.fetch.errorcode.xpath}")
    private String errorCodeXpath;

    @Override
    protected DnBBulkLookupFetcherImpl self() {
        return _self;
    }

    @Override
    protected void executeLookup(DnBBatchMatchContext batchContext) {
        executeLookup(batchContext, keyType(), DnBAPIType.BATCH_FETCH);
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.BATCH;
    }

    @Override
    protected ResponseType getResponseType() {
        return ResponseType.XML;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext) {
        return String.format(getResultUrlFormat, batchContext.getServiceBatchId(), officeID, serviceNumber,
                DateTimeUtils.formatTZ(batchContext.getTimestamp()));
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        headers.add(applicationIdHeader, applicationId);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext) {
        try {
            DnBReturnCode returnCode = parseBatchProcessStatus(response);
            if (returnCode != DnBReturnCode.OK && returnCode != DnBReturnCode.PARTIAL_SUCCESS) {
                batchContext.setDnbCode(returnCode);
                return;
            }
            batchContext.setDnbCode(DnBReturnCode.OK);  // Both OK and PARTIAL_SUCCESS are treated as finished

            parseTimestamp(response, batchContext);

            String contentObjectXpath = String.format(this.contentObjectXpath,
                    (returnCode == DnBReturnCode.PARTIAL_SUCCESS ? "2" : "1"));
            String encodedStr = (String) retrieveXmlValueFromResponse(contentObjectXpath, response);
            byte[] decodeResults = Base64Utils.decodeBase64(encodedStr);
            String[] resultsList = new String(decodeResults).split("\n");
            if (batchContext.getLogDnBBulkResult()) {
                log.info(String.format("Match result for serviceBatchId = %s", batchContext.getServiceBatchId()));
                for (String res : resultsList) {
                    log.info(res);
                }
            }
            for (String result : resultsList) {
                DnBMatchContext normalizedResult = normalizeOneRecord(result, batchContext.getServiceBatchId());
                DnBMatchContext context = batchContext.getContexts().get(normalizedResult.getLookupRequestId());
                context.copyMatchResult(normalizedResult);
            }
            for (String lookupRequestId : batchContext.getContexts().keySet()) {
                DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
                context.setDuration(batchContext.getDuration());
                context.setServiceBatchId(batchContext.getServiceBatchId());
                if (context.getDnbCode() == null) {
                    context.setDnbCode(DnBReturnCode.UNMATCH);
                }
            }
        } catch (Exception ex) {
            log.error(String.format("Fail to extract match result from response of DnB bulk match request %s: %s",
                    batchContext.getServiceBatchId(), response), ex);
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }

    }

    /**
     * DnB used to compress the result in GZIP format. Although the GZIP is
     * removed, keep the decompress method for some time.
     *
     * @param compressed
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unused")
    private List<String> decompress(byte[] compressed) throws IOException {
        List<String> res = new ArrayList<>();
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
        String line;
        while ((line = br.readLine()) != null) {
            res.add(line);
        }
        br.close();
        gis.close();
        bis.close();
        return res;
    }

    private DnBReturnCode parseBatchProcessStatus(String body) {
        String dnBReturnCode = (String) retrieveXmlValueFromResponse(batchResultIdXpath, body);
        switch (dnBReturnCode) {
            case "BC005":
            case "BC007":
                return DnBReturnCode.IN_PROGRESS;
            case "BC001":
                return DnBReturnCode.PARTIAL_SUCCESS;
            case "CM000":
                return DnBReturnCode.OK;
            default:
                return DnBReturnCode.UNKNOWN;
        }
    }

    private DnBMatchContext normalizeOneRecord(String record, String serviceBatchId) {
        DnBMatchContext output = new DnBMatchContext();
        try {
            record = record.substring(1, record.length() - 1);
            String[] values = record.split("\",\"");

            if (values.length < 2) {
                output.setDnbCode(DnBReturnCode.UNMATCH);
                return output;
            }
            String lookupRequestId = StringUtils.strip(values[1]);
            output.setLookupRequestId(lookupRequestId);
            if (values.length < 50) {
                output.setDnbCode(DnBReturnCode.UNMATCH);
                return output;
            }
            if (!StringUtils.isNumeric(StringUtils.strip(values[48]))) {
                output.setDnbCode(DnBReturnCode.UNMATCH);
                return output;
            }
            int confidenceCode = Integer.parseInt(StringUtils.strip(values[48]));
            String matchGrade = StringUtils.strip(values[49]);
            if (StringUtils.isEmpty(matchGrade)) {
                output.setDnbCode(DnBReturnCode.UNMATCH);
                return output;
            }
            String duns = StringUtils.strip(values[25]);
            output.setOrigDuns(duns);
            duns = StringUtils.isNotEmpty(duns) ? duns : null;
            if (duns == null) {
                output.setDnbCode(DnBReturnCode.UNMATCH);
                return output;
            }
            String name = StringUtils.strip(values[26]);
            name = StringUtils.isNotEmpty(name) ? name : null;
            String street = StringUtils.strip(values[29]);
            street = StringUtils.isNotEmpty(street) ? street : null;
            String city = StringUtils.strip(values[31]);
            city = StringUtils.isNotEmpty(city) ? city : null;
            String state = StringUtils.strip(values[36]);
            state = StringUtils.isNotEmpty(state) ? state : null;
            String countryCode = StringUtils.strip(values[32]);
            countryCode = StringUtils.isNotEmpty(countryCode) ? countryCode : null;
            String zipCode = StringUtils.strip(values[33]);
            zipCode = StringUtils.isNotEmpty(zipCode) ? zipCode : null;
            String phoneNumber = StringUtils.strip(values[38]);
            phoneNumber = StringUtils.isNotEmpty(phoneNumber) ? phoneNumber : null;
            String outOfBusiness = StringUtils.strip(values[40]);
            if ("Y".equalsIgnoreCase(outOfBusiness) || "1".equalsIgnoreCase(outOfBusiness)) {
                output.setOutOfBusiness(Boolean.TRUE);
            } else {
                output.setOutOfBusiness(Boolean.FALSE);
            }

            output.setDuns(duns);
            output.setConfidenceCode(confidenceCode);
            output.setMatchGrade(matchGrade);
            NameLocation matchedNameLocation = output.getMatchedNameLocation();
            matchedNameLocation.setName(name);
            matchedNameLocation.setStreet(street);
            matchedNameLocation.setCity(city);
            matchedNameLocation.setState(state);
            matchedNameLocation.setCountryCode(countryCode);
            matchedNameLocation.setZipcode(zipCode);
            matchedNameLocation.setPhoneNumber(phoneNumber);
            output.setDnbCode(DnBReturnCode.OK);
        } catch (Exception e) {
            log.error(String.format("Fail to extract duns from match result of DnB bulk match request %s: %s",
                    serviceBatchId, record), e);
            output.setDnbCode(DnBReturnCode.UNMATCH);
        }
        return output;
    }

    private void parseTimestamp(String response, DnBBatchMatchContext batchContext) {
        String receivedTimeStr = (String) retrieveXmlValueFromResponse(receiveTimestampXpath, response);
        String completeTimeStr = (String) retrieveXmlValueFromResponse(completeTimestampXpath, response);
        Date receivedTime = DateTimeUtils.parseTX(receivedTimeStr);
        Date completeTime = DateTimeUtils.parseTX(completeTimeStr);
        if (completeTime == null || receivedTime == null || completeTime.getTime() <= receivedTime.getTime()) {
            log.warn(String.format("Fail to parse timestamp field in the response of DnB bulk match request  %s",
                    batchContext.getServiceBatchId()));
            batchContext.setDuration(null);
        } else {
            batchContext.setDuration(completeTime.getTime() - receivedTime.getTime());
        }
    }
}
