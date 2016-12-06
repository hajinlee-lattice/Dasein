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
import com.latticeengines.datacloud.match.dnb.DnBAPIType;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
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

    @Value("${datacloud.dnb.realtime.retry.maxattempts}")
    private int retries;

    @Value("${datacloud.dnb.bulk.output.content.object.xpath}")
    private String contentObjectXpath;

    @Value("${datacloud.dnb.bulk.result.id.xpath}")
    private String batchResultIdXpath;

    @Value("${datacloud.dnb.bulk.receive.timestamp.xpath}")
    private String receiveTimestampXpath;

    @Value("${datacloud.dnb.bulk.complete.timestamp.xpath}")
    private String completeTimestampXpath;

    @Value("${datacloud.dnb.bulk.query.interval}")
    private int queryInterval;

    @Value("${datacloud.dnb.bulk.office.id}")
    private int officeID;

    @Value("${datacloud.dnb.bulk.service.number}")
    private int serviceNumber;

    @Value("${datacloud.dnb.bulk.getresult.url.format}")
    private String getResultUrlFormat;

    private static Date timeAnchor = new Date(1);

    private boolean preValidation() {
        Date now = new Date();
        if ((now.getTime() - timeAnchor.getTime()) / (1000 * 60) < queryInterval) {
            return false;
        }
        timeAnchor = now;
        return true;
    }

    @Override
    public DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext) {

        if (!preValidation()) {
            batchContext.setDnbCode(DnBReturnCode.RATE_LIMITING);
            return batchContext;
        }

        for (int i = 0; i < retries; i++) {
            executeLookup(batchContext, DnBKeyType.BATCH, DnBAPIType.BATCH_FETCH);
            if (batchContext.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN || i == retries - 1) {
                if (log.isDebugEnabled()) {
                    log.debug("Fetched result from DnB bulk match api. Status=" + batchContext.getDnbCode());
                }
                if (batchContext.getDnbCode() == DnBReturnCode.OK) {
                    log.info("Successfully fetched results from dnb. Size= " + batchContext.getContexts().size()
                            + " Timestamp=" + batchContext.getTimestamp() + " ServiceId="
                            + batchContext.getServiceBatchId());
                }
                break;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.BATCH);
        }
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

            String lookupRequestId = values[1];
            output.setLookupRequestId(lookupRequestId);
            String duns = values[25];
            if (!StringUtils.isNumeric(values[48])) {
                output.setDnbCode(DnBReturnCode.DISCARD);
                return output;
            }
            int confidenceCode = Integer.parseInt(values[48]);
            String matchGrade = values[49];

            output.setDuns(duns);
            output.setConfidenceCode(confidenceCode);
            output.setMatchGrade(matchGrade);
            output.setDnbCode(DnBReturnCode.OK);
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
