package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class BaseDnBBulkLookupDispatcherImpl extends BaseDnBLookupServiceImpl<DnBBatchMatchContext>
        implements DnBBulkLookupDispatcher {

    private static final Logger log = LoggerFactory.getLogger(BaseDnBBulkLookupDispatcherImpl.class);

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Inject
    protected RateLimitingService rateLimitingService;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Override
    public DnBBatchMatchContext sendRequest(DnBBatchMatchContext batchContext) {
        // Immediate retry is only for case that token expires (Other retryable
        // cases will wait for next schedule)
        for (int i = 0; i < retries; i++) {
            // Don't actually acquire quota because submission might not be
            // successful. Only check quota availability
            RateLimitedAcquisition rlAcq = acquireQuota(batchContext.getContexts().size(),true);
            if (!rlAcq.isAllowed()) {
                logRateLimitingRejection(rlAcq, DnBAPIType.BATCH_DISPATCH);
                batchContext.setDnbCode(DnBReturnCode.RATE_LIMITING);
                return batchContext;
            }
            executeLookup(batchContext);
            if (batchContext.getDnbCode().isSubmittedStatus()) {
                // After request is successfully submitted, log the used quota.
                // Has potential issue that probably during 2 calls of
                // acquireDnBBulkRequest, maybe another job logs its quota and
                // we reach the upper limit of quota. Then quota of this request
                // is not logged.
                // Not a serious issue. We don't need to control DnB quota usage
                // very accurately. If exceeding quota limit, DnB will simply
                // fail the request.
                acquireQuota(batchContext.getContexts().size(), false);
            }
            if (!batchContext.getDnbCode().isImmediateRetryStatus()) {
                log.info("Sent batched request to dnb bulk match api, status={}, size={}, timestamp={}, serviceId={}",
                        batchContext.getDnbCode(), batchContext.getContexts().size(), batchContext.getTimestamp(),
                        batchContext.getServiceBatchId());
                return batchContext;
            }
            log.info("Attempting to refresh {} DnB token which was found invalid: {}", keyType(), batchContext.getToken());
            dnBAuthenticationService.requestToken(keyType(), batchContext.getToken());
        }
        log.error("Failed to submit batched request due to invalid token and failed to refresh");
        return batchContext;
    }

    @Override
    protected void parseError(Exception ex, DnBBatchMatchContext batchContext) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            batchContext.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            log.error(String.format("LedpException in DnB batch dispatching request: %s %s",
                    ((LedpException) ex).getCode().name(), ((LedpException) ex).getCode().getMessage()));
            switch (ledpEx.getCode()) {
            case LEDP_25027:
                batchContext.setDnbCode(DnBReturnCode.UNAUTHORIZED);
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
        parseResponse(response, batchContext);
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        return url();
    }

    @Override
    protected void updateTokenInContext(DnBBatchMatchContext context, String token) {
        context.setToken(token);
    }

    protected abstract String url();

    protected abstract DnBKeyType keyType();

    protected abstract RateLimitedAcquisition acquireQuota(long numRows, boolean attemptOnly);

    protected abstract void executeLookup(DnBBatchMatchContext batchContext);

    protected abstract HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token);

    protected abstract void parseResponse(String response, DnBBatchMatchContext batchContext);

}
