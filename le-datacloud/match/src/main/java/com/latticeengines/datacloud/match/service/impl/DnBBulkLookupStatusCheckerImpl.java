package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;

@Component("dnbBulkLookupStatusChecker")
public class DnBBulkLookupStatusCheckerImpl extends BaseDnBBulkLookupStatusCheckerImpl
        implements DnBBulkLookupStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupStatusCheckerImpl.class);

    @Inject
    private DnBBulkLookupStatusCheckerImpl _self;

    @Value("${datacloud.dnb.bulk.getstatus.url.format}")
    private String urlFormat;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.bulk.getstatus.transactioncode.xpath}")
    private String transactionCodeXpath;

    @Value("${datacloud.dnb.bulk.getstatus.errorcode.xpath}")
    private String errorCodeXpath;

    @Value("${datacloud.dnb.bulk.getstatus.servicebatchid.xpath}")
    private String serviceBatchIdXpath;

    @Value("${datacloud.dnb.bulk.getstatus.status.xpath}")
    private String statusXpath;

    @Value("${datacloud.dnb.bulk.getstatus.batchsize}")
    private int checkStatusBatchSize;

    @Inject
    private RateLimitingService rateLimitingService;

    @Override
    protected DnBBulkLookupStatusCheckerImpl self() {
        return _self;
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.BATCH;
    }

    @Override
    protected int checkStatusBatchSize() {
        return checkStatusBatchSize;
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
    protected RateLimitedAcquisition acquireQuota(boolean attemptOnly) {
        return rateLimitingService.acquireDnBBulkStatus(true);
    }

    @Override
    protected String constructUrl(Map<String, DnBBatchMatchContext> batch) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
            sb.append(entry.getValue().getServiceBatchId() + ".");
        }
        return String.format(urlFormat, sb.substring(0, sb.length() - 1), DateTimeUtils.formatTZ(new Date()));
    }

    @Override
    protected HttpEntity<String> constructEntity(Map<String, DnBBatchMatchContext> batch, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, token);
        headers.add(applicationIdHeader, applicationId);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseResponse(String response, Map<String, DnBBatchMatchContext> batches) {
        StringBuilder sb = new StringBuilder();
        for (String serviceBatchId : batches.keySet()) {
            sb.append(serviceBatchId + " ");
        }
        DnBReturnCode transactionReturnCode = parseTransactionStatus(response);
        if (transactionReturnCode != DnBReturnCode.OK) {
            updateReturnCodes(batches, transactionReturnCode);
            log.error(
                    String.format("Fail to check status for DnB bulk match requests %s: %s", sb.toString(), response));
            return;
        }
        for (int i = 1; i <= batches.size(); i++) {
            String serviceBatchId = (String) retrieveXmlValueFromResponse(
                    String.format(serviceBatchIdXpath, i), response);
            String status = (String) retrieveXmlValueFromResponse(String.format(statusXpath, i),
                    response);
            if (StringUtils.isNotEmpty(serviceBatchId) && batches.containsKey(serviceBatchId)) {
                batches.get(serviceBatchId).setDnbCode(parseBatchStatus(status));
            }
        }
    }

    private DnBReturnCode parseTransactionStatus(String body) {
        String code = (String) retrieveXmlValueFromResponse(transactionCodeXpath, body);
        switch (code) {
        case "CM000":
            return DnBReturnCode.OK;
        default:
            return DnBReturnCode.UNKNOWN;
        }
    }

    private DnBReturnCode parseBatchStatus(String status) {
        switch (status) {
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

    private void updateReturnCodes(Map<String, DnBBatchMatchContext> batches, DnBReturnCode code) {
        for (Map.Entry<String, DnBBatchMatchContext> entry : batches.entrySet()) {
            entry.getValue().setDnbCode(code);
        }
    }

}
