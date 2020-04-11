package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class BaseDnBBulkLookupStatusCheckerImpl extends BaseDnBLookupServiceImpl<Map<String, DnBBatchMatchContext>>
        implements DnBBulkLookupStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(BaseDnBBulkLookupStatusCheckerImpl.class);

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Override
    public List<DnBBatchMatchContext> checkStatus(List<DnBBatchMatchContext> batchContexts) {
        int count = 0;
        List<DnBBatchMatchContext> shuffledBatchContexts = new ArrayList<>(batchContexts);
        Collections.shuffle(shuffledBatchContexts);
        StringBuilder logBuilder = new StringBuilder();
        while (count < shuffledBatchContexts.size()) {
            // BatchID -> DnBBatchMatchContext
            Map<String, DnBBatchMatchContext> batches = new HashMap<>();
            for (int i = count; i < Math.min(shuffledBatchContexts.size(), count + checkStatusBatchSize()); i++) {
                batches.put(shuffledBatchContexts.get(i).getServiceBatchId(), shuffledBatchContexts.get(i));
                count++;
            }
            for (int i = 0; i < retries; i++) {
                RateLimitedAcquisition rlAcq = acquireQuota(true);
                if (!rlAcq.isAllowed()) {
                    logRateLimitingRejection(rlAcq, DnBAPIType.BATCH_STATUS);
                    break;
                }
                executeLookup(batches, keyType(), DnBAPIType.BATCH_STATUS);
                if (batches.entrySet().iterator().next().getValue().getDnbCode().isNormalStatus()) {
                    acquireQuota(false);
                }
                logBuilder.delete(0, logBuilder.length());
                for (Map.Entry<String, DnBBatchMatchContext> entry : batches.entrySet()) {
                    long mins = (System.currentTimeMillis() - entry.getValue().getTimestamp().getTime()) / 60 / 1000;
                    logBuilder.append(String.format("%s:%s(%d mins, %d records)%s ", entry.getValue().getServiceBatchId(),
                            entry.getValue().getDnbCode(), mins, entry.getValue().getContexts().size(),
                            StringUtils.isEmpty(entry.getValue().getRetryForServiceBatchId()) ? ""
                                    : " (retry for " + entry.getValue().getRetryForServiceBatchId() + ")"));
                }
                if (!batches.entrySet().iterator().next().getValue().getDnbCode().isImmediateRetryStatus()) {
                    log.info("Checked status for batch requests: " + logBuilder.toString());
                    break;
                }
                log.info("Attempting to refresh DnB token which was found invalid: "
                        + batches.entrySet().iterator().next().getValue().getToken());
                dnBAuthenticationService.requestToken(keyType(),
                        batches.entrySet().iterator().next().getValue().getToken());
                if (i == retries - 1) {
                    log.error("Fail to check status for batch requests due to invalid token and failed to refresh: "
                            + logBuilder.toString());
                }
            }
        }
        return batchContexts;
    }

    @Override
    protected String constructUrl(Map<String, DnBBatchMatchContext> batch, DnBAPIType apiType) {
        return constructUrl(batch);
    }

    @Override
    protected void parseResponse(String response, Map<String, DnBBatchMatchContext> batches, DnBAPIType apiType) {
        parseResponse(response, batches);
    }

    @Override
    protected void updateTokenInContext(Map<String, DnBBatchMatchContext> contexts, String token) {
        for (DnBBatchMatchContext context : contexts.values()) {
            context.setToken(token);
        }
    }

    @Override
    protected void parseError(Exception ex, Map<String, DnBBatchMatchContext> batch) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error(String.format("HttpClientErrorException in DnB batch status checking request: HttpStatus %d %s",
                    ((HttpClientErrorException) ex).getStatusCode().value(),
                    ((HttpClientErrorException) ex).getStatusCode().name()));
            for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                entry.getValue().setDnbCode(parseDnBHttpError(httpEx));
            }
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            log.error("LedpException in DnB batch status checking: {} {}", ledpEx.getCode().name(),
                    ledpEx.getCode().getMessage());
            switch (ledpEx.getCode()) {
            case LEDP_25027:
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.UNAUTHORIZED);
                }
                break;
            case LEDP_25037:
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.BAD_REQUEST);
                }
                break;
            case LEDP_25039:
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.SERVICE_UNAVAILABLE);
                }
                break;
            default:
                for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                    entry.getValue().setDnbCode(DnBReturnCode.UNKNOWN);
                }
                break;
            }
        } else {
            log.error("Unhandled exception in DnB batch status checking: " + ex.getMessage(), ex);
            for (Map.Entry<String, DnBBatchMatchContext> entry : batch.entrySet()) {
                entry.getValue().setDnbCode(DnBReturnCode.UNKNOWN);
            }
        }
    }

    protected abstract DnBKeyType keyType();

    protected abstract int checkStatusBatchSize();

    protected abstract RateLimitedAcquisition acquireQuota(boolean attemptOnly);

    protected abstract String constructUrl(Map<String, DnBBatchMatchContext> batch);

    protected abstract void parseResponse(String response, Map<String, DnBBatchMatchContext> batches);

}
