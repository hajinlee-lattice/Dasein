package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;

import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class BaseDnBBulkLookupFetcherImpl extends BaseDnBLookupServiceImpl<DnBBatchMatchContext>
        implements DnBBulkLookupFetcher {

    private static final Logger log = LoggerFactory.getLogger(BaseDnBBulkLookupFetcherImpl.class);

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Value("${datacloud.dnb.retry.maxattempts}")
    private int retries;

    @Override
    public DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext) {
        for (int i = 0; i < retries; i++) {
            // Don't check rate limit because every request could call status
            // check API multiple times (successfully), but only fetch result
            // API single time (successfully).
            // Both 2 APIs' quota limit is the same. As long as we have rate
            // limit control on status check API, we don't need to have control
            // here
            // If DnB API response complains we exceed rate limit, we will wait
            // for next scheduled fetch to retry
            executeLookup(batchContext);
            if (!batchContext.getDnbCode().isImmediateRetryStatus()) {
                if (batchContext.getDnbCode() == DnBReturnCode.OK) {
                    log.info("Successfully fetched batch results from dnb. Size=" + batchContext.getContexts().size()
                            + " Timestamp=" + batchContext.getTimestamp() + " ServiceId="
                            + batchContext.getServiceBatchId());
                } else {
                    log.error("Encountered issue in fetching batch results from dnb. DnBCode="
                            + batchContext.getDnbCode());
                }
                return batchContext;
            }
            log.info("Attempting to refresh DnB token which was found invalid: " + batchContext.getToken());
            dnBAuthenticationService.requestToken(keyType(), batchContext.getToken());
        }
        log.error("Fail to fetch batch results from dnb due to invalid token and failed to refresh");
        return batchContext;
    }

    @Override
    protected void parseError(Exception ex, DnBBatchMatchContext batchContext) {
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpEx = (HttpClientErrorException) ex;
            log.error(String.format("HttpClientErrorException in DnB batch fetching request: HttpStatus %d %s",
                    ((HttpClientErrorException) ex).getStatusCode().value(),
                    ((HttpClientErrorException) ex).getStatusCode().name()));
            batchContext.setDnbCode(parseDnBHttpError(httpEx));
        } else if (ex instanceof LedpException) {
            LedpException ledpEx = (LedpException) ex;
            log.error(String.format("LedpException in DnB batch match fetching request: %s %s",
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
            log.error("Unhandled exception in DnB batch match fetching request: " + ex.getMessage(), ex);
            batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
        }
    }

    @Override
    protected void parseResponse(String response, DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        try {
            parseResponse(response, batchContext);
        } catch (Exception ex) {
            log.error(String.format("Fail to extract match result from response of DnB bulk match request %s: %s",
                    batchContext.getServiceBatchId(), response), ex);
            batchContext.setDnbCode(DnBReturnCode.BAD_RESPONSE);
        }

    }

    @Override
    protected void updateTokenInContext(DnBBatchMatchContext context, String token) {
        context.setToken(token);
    }

    @Override
    protected String constructUrl(DnBBatchMatchContext batchContext, DnBAPIType apiType) {
        return constructUrl(batchContext);
    }

    protected abstract String constructUrl(DnBBatchMatchContext batchContext);

    protected abstract DnBKeyType keyType();

    protected abstract void executeLookup(DnBBatchMatchContext batchContext);

    protected abstract HttpEntity<String> constructEntity(DnBBatchMatchContext batchContext, String token);

    protected abstract void parseResponse(String response, DnBBatchMatchContext batchContext);

}
