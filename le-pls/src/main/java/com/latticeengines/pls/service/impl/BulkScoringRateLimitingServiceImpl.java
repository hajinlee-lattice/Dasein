package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.locks.RateLimitedResourceManager;
import com.latticeengines.domain.exposed.camille.locks.RateLimitDefinition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.pls.service.BulkScoringRateLimitingService;

@Component("bulkScoringRateLimitingService")
public class BulkScoringRateLimitingServiceImpl implements BulkScoringRateLimitingService {

    private static final Logger log = LoggerFactory.getLogger(BulkScoringRateLimitingServiceImpl.class);
    public static final String BULK_SCORING_REQUEST_REGULATOR = "BulkScoringRequest";

    private static final String COUNTER_REQUEST = "Request";
    private static final String COUNTER_ROWS = "Rows";

    private static boolean bulkRequestRegulatorRegistered = false;

    @Value("${pls.scoring..bulk.requests.per.hour}")
    private int bulkRequestsPerHour;

    @Value("${pls.scoring..bulk.rows.per.hour}")
    private int bulkRowsPerHour;

    @Override
    public RateLimitedAcquisition acquireBulkRequest(String tenant, long numRows, boolean attemptOnly) {
        if (!bulkRequestRegulatorRegistered) {
            registerBulkRequestRegulator(tenant);
        }
        Map<String, Long> quantities = new HashMap<>();
        quantities.put(COUNTER_REQUEST, 1L);
        quantities.put(COUNTER_ROWS, numRows);
        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(getResourceName(tenant), quantities, 5,
                TimeUnit.SECONDS, attemptOnly);
        logRejectedAcquisition(acquisition, getResourceName(tenant));
        return acquisition;
    }

    private void registerBulkRequestRegulator(String tenant) {
        RateLimitDefinition definition = RateLimitDefinition.crossDivisionDefinition();
        definition.addQuota(COUNTER_REQUEST, new RateLimitDefinition.Quota(bulkRequestsPerHour, 1, TimeUnit.HOURS));
        definition.addQuota(COUNTER_ROWS, new RateLimitDefinition.Quota(bulkRowsPerHour, 1, TimeUnit.HOURS));
        RateLimitedResourceManager.registerResource(getResourceName(tenant), definition);
        bulkRequestRegulatorRegistered = true;
    }

    private String getResourceName(String tenant) {
        return BULK_SCORING_REQUEST_REGULATOR + "_" + tenant;
    }

    private void logRejectedAcquisition(RateLimitedAcquisition acquisition, String regulatorName) {
        if (!acquisition.isAllowed()) {
            String logMessage = "Acquisition of " + regulatorName + " regulator is rejected.";
            if (acquisition.getExceedingQuotas() != null && !acquisition.getExceedingQuotas().isEmpty()) {
                logMessage += " Exceeding quotas: " + StringUtils.join(acquisition.getExceedingQuotas(), ",");
            }
            log.info(logMessage);
        }
    }

}
