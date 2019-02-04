package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbBulkLookupDispatcher")
public class DnBBulkLookupDispatcherImpl extends BaseDnBLookupServiceImpl<DnBBatchMatchContext>
        implements DnBBulkLookupDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupDispatcherImpl.class);

    private static final String DNB_BULK_BODY_FILE_NAME = "com/latticeengines/datacloud/match/BulkApiBodyTemplate.xml";

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Autowired
    private RateLimitingService rateLimitingService;

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

    @Value("${datacloud.dnb.bulk.servicebatchid.xpath}")
    private String serviceIdXpath;

    @Value("${datacloud.dnb.bulk.input.record.format}")
    private String recordFormat;

    private String dnBBulkApiBody;

    @PostConstruct
    public void init() throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(DNB_BULK_BODY_FILE_NAME);
        if (is == null) {
            throw new RuntimeException("Cannot find resource " + DNB_BULK_BODY_FILE_NAME);
        }
        dnBBulkApiBody = IOUtils.toString(is, "UTF-8");
    }

    @Override
    public DnBBatchMatchContext sendRequest(DnBBatchMatchContext batchContext) {
        for (int i = 0; i < retries; i++) {
            // Don't actually acquire quota because submission might not be
            // successful. Only check quota availability
            RateLimitedAcquisition rlAcq = rateLimitingService.acquireDnBBulkRequest(batchContext.getContexts().size(),
                    true);
            if (!rlAcq.isAllowed()) {
                logRateLimitingRejection(rlAcq, DnBAPIType.BATCH_DISPATCH);
                batchContext.setDnbCode(DnBReturnCode.RATE_LIMITING);
                return batchContext;
            }
            executeLookup(batchContext, DnBKeyType.BATCH, DnBAPIType.BATCH_DISPATCH);
            if (batchContext.getDnbCode() == DnBReturnCode.SUBMITTED) {
                // After request is successfully submitted, log the used quota.
                // Has potential issue that probably during 2 calls of
                // acquireDnBBulkRequest, maybe another job logs its quota and
                // we reach the upper limit of quota. Then quota of this request
                // is not logged.
                // Not a serious issue. We don't need to control DnB quota usage
                // very accurately. If exceeding quota limit, DnB will simply
                // fail the request.
                rateLimitingService.acquireDnBBulkRequest(batchContext.getContexts().size(), false);
            }
            if (batchContext.getDnbCode() != DnBReturnCode.EXPIRED_TOKEN) {
                log.info("Sent batched request to dnb bulk match api, status=" + batchContext.getDnbCode() + " size="
                        + batchContext.getContexts().size() + " timestamp=" + batchContext.getTimestamp()
                        + " serviceId=" + batchContext.getServiceBatchId());
                return batchContext;
            }
            dnBAuthenticationService.refreshToken(DnBKeyType.BATCH);
        }
        log.error("Fail to submit batched request because API token expires and fails to refresh");
        return batchContext;
    }

    @Override
    protected void parseError(Exception ex, DnBBatchMatchContext batchContext) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error(String.format("HttpClientErrorException in DnB batch match dispatching request: HttpStatus %d %s",
                    ((HttpClientErrorException) ex).getStatusCode().value(),
                    ((HttpClientErrorException) ex).getStatusCode().name()));
            batchContext.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            log.error(String.format("LedpException in DnB batch match dispatching request: %s %s",
                    ((LedpException) ex).getCode().name(), ((LedpException) ex).getCode().getMessage()));
            switch (ledpEx.getCode()) {
            case LEDP_25027:
                batchContext.setDnbCode(DnBReturnCode.EXPIRED_TOKEN);
                break;
            case LEDP_25037:
                batchContext.setDnbCode(DnBReturnCode.BAD_REQUEST);
                break;
            case LEDP_25039:
                batchContext.setDnbCode(DnBReturnCode.SERVICE_UNAVAILABLE);
                break;
            default:
                batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
                break;
            }
        } else {
            log.error("Unhandled exception in DnB batch match dispatching request: " + ex.getMessage(), ex);
            batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
        }

    }


    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        String serviceBatchId = (String) retrieveXmlValueFromResponse(serviceIdXpath, response);
        if (!StringUtils.isEmpty(serviceBatchId)) {
            batchContext.setServiceBatchId(serviceBatchId);
            batchContext.setDnbCode(DnBReturnCode.SUBMITTED);
        } else {
            log.error(String.format("Fail to extract serviceBatchId from response of DnB bulk match request: %s",
                    response));
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }
    }

    @Override
    protected HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token) {
        String body = constructBulkRequestBody(batchContext);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        headers.add(applicationIdHeader, applicationId);
        headers.add(authorizationHeader, token);

        HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
        return requestEntity;
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        return url;
    }

    private String constructBulkRequestBody(DnBBatchMatchContext batchContext) {
        Date now = new Date();
        batchContext.setTimestamp(now);
        String createdDateUTC = DateTimeUtils.formatTZ(now);
        String tupleStr = convertTuplesToString(batchContext);
        String inputObjectBase64 = Base64Utils.encodeBase64(tupleStr, false, Integer.MAX_VALUE);
        if (batchContext.getLogDnBBulkResult()) {
            log.info(String.format("Submitted encoded match input: %s", inputObjectBase64));
        }
        return String.format(dnBBulkApiBody, createdDateUTC.toString(), inputObjectBase64,
                String.valueOf(batchContext.getContexts().size()));
    }

    private String convertTuplesToString(DnBBatchMatchContext batchContext) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, DnBMatchContext> entry : batchContext.getContexts().entrySet()) {
            String recordStr = constructOneRecord(entry.getKey(), entry.getValue());
            sb.append(recordStr);
            sb.append("\n");
        }
        return sb.toString();
    }

    private String constructOneRecord(String transactionId, DnBMatchContext matchContext) {
        return String.format(recordFormat, transactionId,
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getName(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getCity(), ""),
                StringUtils.defaultIfEmpty(
                        LocationUtils.getStardardStateCode(matchContext.getInputNameLocation().getCountry(),
                                matchContext.getInputNameLocation().getState()),
                        ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getZipcode(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getCountryCode(), ""),
                StringUtils.defaultIfEmpty(matchContext.getInputNameLocation().getPhoneNumber(), ""));
    }


}
