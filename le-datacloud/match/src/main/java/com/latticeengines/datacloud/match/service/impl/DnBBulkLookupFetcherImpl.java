package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbBulkLookupFetcher")
public class DnBBulkLookupFetcherImpl extends BaseDnBLookupServiceImpl<DnBBatchMatchContext>
        implements DnBBulkLookupFetcher {

    private static final Log log = LogFactory.getLog(DnBBulkLookupFetcherImpl.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Value("${datacloud.dnb.bulk.url}")
    private String url;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

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

    @Override
    public DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext) {
        for (int i = 0; i < retries; i++) {
            executeLookup(batchContext, DnBKeyType.BATCH, DnBAPIType.BATCH_FETCH);
            if (batchContext.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN) {
                if (batchContext.getDnbCode() == DnBReturnCode.OK) {
                    log.info("Successfully fetched batch results from dnb. Size= " + batchContext.getContexts().size()
                            + " Timestamp=" + batchContext.getTimestamp() + " ServiceId="
                            + batchContext.getServiceBatchId());
                } else {
                    log.info("Encountered issue in fetching batch results from dnb. DnBCode="
                            + batchContext.getDnbCode().getMessage());
                }
                return batchContext;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.BATCH);
        }
        log.error("Fail to fetch batch results from dnb because API token expires and fails to refresh");
        return batchContext;
    }

    @Override
    protected void parseError(Exception ex, DnBBatchMatchContext batchContext) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            if (log.isDebugEnabled()) {
                log.debug("HttpClientErrorException in DnB batch match fetching request: " + httpEx.getStatusText());
            }
            batchContext.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            if (log.isDebugEnabled()) {
                log.debug("LedpException in DnB batch match fetching request: " + ledpEx.getCode().getMessage());
            }
            if (ledpEx.getCode() == LedpCode.LEDP_25027) {
                batchContext.setDnbCode(DnBReturnCode.EXPIRED_TOKEN);
            } else {
                batchContext.setDnbCode(DnBReturnCode.BAD_REQUEST);
            }
        } else {
            log.warn("Unhandled exception in DnB batch match fetching request: " + ex.getMessage());
            ex.printStackTrace();
            batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
        }
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        try {
            DnBReturnCode returnCode = parseBatchProcessStatus(response);
            batchContext.setDnbCode(returnCode);

            if (batchContext.getDnbCode() != DnBReturnCode.OK) {
                return;
            }

            parseTimestamp(response, batchContext);

            String encodedStr = (String) retrieveXmlValueFromResponse(contentObjectXpath, response);
            byte[] decodeResults = Base64Utils.decodeBase64(encodedStr);
            String decodedStr = new String(decodeResults);
            if (batchContext.getLogDnBBulkResult()) {
                log.info(String.format("Match result for serviceBatchId = %s:\n%s", batchContext.getServiceBatchId(),
                        decodedStr));
            }
            List<String> resultsList = Arrays.asList(decodedStr.split("\n"));
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
            log.warn(String.format("Fail to extract match result from response of DnB bulk match request %s: %s",
                    batchContext.getServiceBatchId(), response));
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }

    }

    private DnBReturnCode parseBatchProcessStatus(String body) {
        String dnBReturnCode = (String) retrieveXmlValueFromResponse(batchResultIdXpath, body);
        switch (dnBReturnCode) {
        case "BC005":
        case "BC007":
            return DnBReturnCode.IN_PROGRESS;
        case "BC001":
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

            if (values.length < 50) {
                output.setDnbCode(DnBReturnCode.DISCARD);
                return output;
            }
            if (!StringUtils.isNumeric(StringUtils.strip(values[48]))) {
                output.setDnbCode(DnBReturnCode.DISCARD);
                return output;
            }
            int confidenceCode = Integer.parseInt(StringUtils.strip(values[48]));
            String matchGrade = StringUtils.strip(values[49]);
            if (StringUtils.isEmpty(matchGrade)) {
                output.setDnbCode(DnBReturnCode.DISCARD);
                return output;
            }
            String lookupRequestId = StringUtils.strip(values[1]);
            output.setLookupRequestId(lookupRequestId);
            String duns = StringUtils.strip(values[25]);
            duns = StringUtils.isNotEmpty(duns) ? duns : null;
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
            output.setDnbCode(DnBReturnCode.OK);
            NameLocation matchedNameLocation = output.getMatchedNameLocation();
            matchedNameLocation.setName(name);
            matchedNameLocation.setStreet(street);
            matchedNameLocation.setCity(city);
            matchedNameLocation.setState(state);
            matchedNameLocation.setCountryCode(countryCode);
            matchedNameLocation.setZipcode(zipCode);
            matchedNameLocation.setPhoneNumber(phoneNumber);
            dnbMatchResultValidator.validate(output);
        } catch (Exception e) {
            log.warn(String.format("Fail to extract duns from match result of DnB bulk match request %s: %s",
                    serviceBatchId, record));
            output.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }
        return output;
    }

    private void parseTimestamp(String response, DnBBatchMatchContext batchContext) {
        String receivedTimeStr = (String) retrieveXmlValueFromResponse(receiveTimestampXpath, response);
        String completeTimeStr = (String) retrieveXmlValueFromResponse(completeTimestampXpath, response);
        Date receivedTime = DateTimeUtils.parseT(receivedTimeStr);
        Date completeTime = DateTimeUtils.parseT(completeTimeStr);
        if (completeTime == null || receivedTime == null || completeTime.getTime() <= receivedTime.getTime()) {
            log.warn(String.format("Fail to parse timestamp field in the response of DnB bulk match request  %s",
                    batchContext.getServiceBatchId()));
            batchContext.setDuration(null);
        } else {
            batchContext.setDuration(completeTime.getTime() - receivedTime.getTime());
        }
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext, DnBAPIType apiType) {
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
}
