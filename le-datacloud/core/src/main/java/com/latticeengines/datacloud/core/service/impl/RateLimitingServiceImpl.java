package com.latticeengines.datacloud.core.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.locks.RateLimitedResourceManager;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.domain.exposed.camille.locks.RateLimitDefinition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

@Component("rateLimitingService")
public class RateLimitingServiceImpl implements RateLimitingService {

    private static final Logger log = LoggerFactory.getLogger(RateLimitingServiceImpl.class);
    private static final String DNB_BULK_REQUEST_REGULATOR = "DnBBulkRequest";
    private static final String DNB_BULK_STATUS_REGULATOR = "DnBBulkStatus";

    private static final String COUNTER_REQUEST = "Request";
    private static final String COUNTER_ROWS = "Rows";

    private static boolean dnbBulkRequestRegulatorRegistered = false;
    private static boolean dnbBulkStatusRegulatorRegistered = false;

    @Value("${datacloud.dnb.bulk.requests.per.second}")
    private int bulkRequestsPerSecond;

    @Value("${datacloud.dnb.bulk.requests.per.hour}")
    private int bulkRequestsPerHour;

    @Value("${datacloud.dnb.bulk.rows.per.hour}")
    private int bulkRowsPerHour;

    @Value("${datacloud.dnb.bulk.status.request.per.second}")
    private int bulkStatusPerSecond;

    @Value("${datacloud.dnb.bulk.status.request.per.hour}")
    private int bulkStatusPerHour;

    @Override
    public RateLimitedAcquisition acquireDnBBulkRequest(long numRows, boolean attemptOnly) {
        if (!dnbBulkRequestRegulatorRegistered) {
            registerDnBBulkRequestRegulator();
        }
        Map<String, Long> inquiringQuantities = new HashMap<>();
        inquiringQuantities.put(COUNTER_REQUEST, 1L);
        inquiringQuantities.put(COUNTER_ROWS, numRows);
        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(DNB_BULK_REQUEST_REGULATOR,
                inquiringQuantities, 1, TimeUnit.SECONDS, attemptOnly);
        logRejectedAcquisition(acquisition, DNB_BULK_REQUEST_REGULATOR);
        return acquisition;
    }

    @Override
    public RateLimitedAcquisition acquireDnBBulkStatus(boolean attemptOnly) {
        if (!dnbBulkStatusRegulatorRegistered) {
            registerDnBBulkStatusRegulator();
        }
        Map<String, Long> inquiringQuantities = new HashMap<>();
        inquiringQuantities.put(COUNTER_REQUEST, 1L);
        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(DNB_BULK_STATUS_REGULATOR,
                inquiringQuantities, 1, TimeUnit.SECONDS, attemptOnly);
        logRejectedAcquisition(acquisition, DNB_BULK_STATUS_REGULATOR);
        return acquisition;
    }

    private void registerDnBBulkRequestRegulator() {
        RateLimitDefinition definition = RateLimitDefinition.crossDivisionDefinition();
        definition.addQuota(COUNTER_REQUEST, new RateLimitDefinition.Quota(bulkRequestsPerSecond, 1, TimeUnit.SECONDS));
        definition.addQuota(COUNTER_REQUEST, new RateLimitDefinition.Quota(bulkRequestsPerHour, 1, TimeUnit.HOURS));
        definition.addQuota(COUNTER_ROWS, new RateLimitDefinition.Quota(bulkRowsPerHour, 1, TimeUnit.HOURS));
        RateLimitedResourceManager.registerResource(DNB_BULK_REQUEST_REGULATOR, definition);
        dnbBulkRequestRegulatorRegistered = true;
    }

    private void registerDnBBulkStatusRegulator() {
        RateLimitDefinition definition = RateLimitDefinition.crossDivisionDefinition();
        definition.addQuota(COUNTER_REQUEST, new RateLimitDefinition.Quota(bulkStatusPerSecond, 1, TimeUnit.SECONDS));
        definition.addQuota(COUNTER_REQUEST, new RateLimitDefinition.Quota(bulkStatusPerHour, 1, TimeUnit.HOURS));
        RateLimitedResourceManager.registerResource(DNB_BULK_STATUS_REGULATOR, definition);
        dnbBulkStatusRegulatorRegistered = true;
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
