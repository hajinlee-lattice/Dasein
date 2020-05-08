package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;

@Component("dnbBulkLookupDispatcher")
public class DnBBulkLookupDispatcherImpl extends BaseDnBBulkLookupDispatcherImpl implements DnBBulkLookupDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupDispatcherImpl.class);

    private static final String DNB_BULK_BODY_FILE_NAME = "com/latticeengines/datacloud/match/BulkApiBodyTemplate.xml";

    @Inject
    private DnBBulkLookupDispatcherImpl _self;

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

    @Value("${datacloud.dnb.bulk.dispatch.errorcode.xpath}")
    private String errorCodeXpath;

    @Value("${datacloud.dnb.bulk.input.record.format}")
    private String recordFormat;

    private String dnBBulkApiBody;

    @Override
    protected DnBBulkLookupDispatcherImpl self() {
        return _self;
    }

    @PostConstruct
    public void init() throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(DNB_BULK_BODY_FILE_NAME);
        if (is == null) {
            throw new RuntimeException("Cannot find resource " + DNB_BULK_BODY_FILE_NAME);
        }
        dnBBulkApiBody = IOUtils.toString(is, Charset.defaultCharset());
    }

    @Override
    protected String url() {
        return url;
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.BATCH;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected ResponseType getResponseType() {
        return ResponseType.XML;
    }

    @Override
    protected RateLimitedAcquisition acquireQuota(long numRows, boolean attemptOnly) {
        return rateLimitingService.acquireDnBBulkRequest(numRows, attemptOnly);
    }

    @Override
    protected void executeLookup(DnBBatchMatchContext batchContext) {
        executeLookup(batchContext, keyType(), DnBAPIType.BATCH_DISPATCH);
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext) {
        String serviceBatchId = (String) retrieveXmlValueFromResponse(serviceIdXpath, response);
        if (!StringUtils.isEmpty(serviceBatchId)) {
            batchContext.setServiceBatchId(serviceBatchId);
            batchContext.setDnbCode(DnBReturnCode.SUBMITTED);
        } else {
            log.error("Fail to extract serviceBatchId from response of DnB bulk match request: {}", response);
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
        return new HttpEntity<>(body, headers);
    }

    private String constructBulkRequestBody(DnBBatchMatchContext batchContext) {
        Date now = new Date();
        batchContext.setTimestamp(now);
        String createdDateUTC = DateTimeUtils.formatTZ(now);
        String tupleStr = convertTuplesToString(batchContext);
        String inputObjectBase64 = Base64Utils.encodeBase64(tupleStr, false, Integer.MAX_VALUE);
        if (batchContext.getLogDnBBulkResult()) {
            log.info("Submitted encoded match input: {}", inputObjectBase64);
        }
        return String.format(dnBBulkApiBody, createdDateUTC, inputObjectBase64, batchContext.getContexts().size());
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
