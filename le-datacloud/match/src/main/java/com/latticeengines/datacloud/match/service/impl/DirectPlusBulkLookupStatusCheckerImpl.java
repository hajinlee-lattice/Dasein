package com.latticeengines.datacloud.match.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("directPlusBulkLookupStatusChecker")
public class DirectPlusBulkLookupStatusCheckerImpl extends BaseDnBBulkLookupStatusCheckerImpl
        implements DnBBulkLookupStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusBulkLookupStatusCheckerImpl.class);

    @Inject
    private DirectPlusBulkLookupStatusCheckerImpl _self;

    @Value("${datacloud.dnb.direct.plus.batch.status.url}")
    private String url;

    @Value("${datacloud.dnb.direct.plus.batch.s3.key}")
    private String s3Key;

    @Value("${datacloud.dnb.authorization.header}")
    private String authorizationHeader;

    @Value("${datacloud.dnb.direct.plus.errorcode.jsonpath}")
    private String errorCodeXpath;

    @Inject
    private RateLimitingService rateLimitingService;

    @Override
    protected DirectPlusBulkLookupStatusCheckerImpl self() {
        return _self;
    }

    @Override
    protected DnBKeyType keyType() {
        return DnBKeyType.DPLUS;
    }

    @Override
    protected int checkStatusBatchSize() {
        return 1;
    }

    @Override
    protected ResponseType getResponseType() {
        return ResponseType.JSON;
    }

    @Override
    protected String getErrorCodePath() {
        return errorCodeXpath;
    }

    @Override
    protected RateLimitedAcquisition acquireQuota(boolean attemptOnly) {
        return rateLimitingService.acquireDirectPlusBatchStatus(attemptOnly);
    }

    @Override
    protected String constructUrl(Map<String, DnBBatchMatchContext> batch) {
        StringBuilder sb = new StringBuilder(url).append("/");
        for (String jobId: batch.keySet()) {
            sb.append(jobId);
            break;
        }
        return sb.toString();
    }

    @Override
    protected HttpEntity<String> constructEntity(Map<String, DnBBatchMatchContext> batch, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(authorizationHeader, "Bearer " + token);
        headers.add("Customer-Key", s3Key);
        return new HttpEntity<>("", headers);
    }

    @Override
    protected void parseResponse(String response, Map<String, DnBBatchMatchContext> batches) {
        String jobId = null;
        for (String serviceBatchId : batches.keySet()) {
            jobId = serviceBatchId;
            break;
        }
        DnBBatchMatchContext batch = batches.get(jobId);
        JsonNode json = JsonUtils.deserialize(response, JsonNode.class);
        DnBReturnCode returnCode = parseBatchStatus(json);
        batch.setDnbCode(returnCode);
        String resultUrl = JsonUtils.parseStringValueAtPath(json, "outputDetail", "contentURL");
        batch.setResultUrl(resultUrl);
        log.info("Multi-Process request {} is in the status of {}", jobId, returnCode);
    }

    private DnBReturnCode parseBatchStatus(JsonNode response) {
        String code = JsonUtils.parseStringValueAtPath(response, "information", "code");
        switch (code) {
        case "60101": // Requested
            return DnBReturnCode.SUBMITTED;
        case "60102": // Processing
            return DnBReturnCode.IN_PROGRESS;
        case "60104": // Processed
            return DnBReturnCode.OK;
        default:
            return DnBReturnCode.UNKNOWN;
        }
    }

}
