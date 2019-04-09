package com.latticeengines.playmaker.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.locks.RateLimitedResourceManager;
import com.latticeengines.domain.exposed.camille.locks.RateLimitDefinition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.playmaker.service.PlaymakerRateLimitingService;

@Component("playmakerRateLimitingService")
public class PlaymakerRateLimitingServiceImpl implements PlaymakerRateLimitingService {

    private static final Logger log = LoggerFactory.getLogger(PlaymakerRateLimitingServiceImpl.class);

    public static final String PLAYMAKER_REQUEST_REGULATOR = "PlaymakerRequest";

    private static final String COUNTER_REQUEST_PER_HOUR = "PlaymakerRequestPerHour";
    private static final String COUNTER_REQUEST_PER_MINUTE = "PlaymakerRequestPerMinute";

    private static final String PRODUCTION_ENVIRONMENT = "prod";

    private static boolean playmakerRequestRegulatorRegistered = false;

    @Value("${playmaker.requests.per.hour.max:1000}")
    private int playmakerRequestsPerHour;

    @Value("${playmaker.requests.per.minute.max:20}")
    private int playmakerRequestsPerMinute;

    @Value("${playmaker.requests.per.minute.max.test:100}")
    private int playmakerRequestsPerMinuteTest;

    @Value("${playmaker.environment:prod}")
    private String playmakerEnvironment;

    @Override
    public RateLimitedAcquisition acquirePlaymakerRequest(String tenant, boolean attemptOnly) {
        if (!playmakerRequestRegulatorRegistered) {
            registerPlaymakerRequestRegulator(tenant);
        }
        Map<String, Long> quantities = new HashMap<>();
        quantities.put(COUNTER_REQUEST_PER_HOUR, 1L);
        quantities.put(COUNTER_REQUEST_PER_MINUTE, 1L);
        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(getResourceName(tenant), quantities, 5,
                TimeUnit.SECONDS, attemptOnly);
        logRejectedAcquisition(acquisition, getResourceName(tenant));
        return acquisition;
    }

    private void registerPlaymakerRequestRegulator(String tenant) {
        int perMinuteQuota = (isTestTenant(tenant) && playmakerEnvironment != PRODUCTION_ENVIRONMENT)? playmakerRequestsPerMinuteTest : playmakerRequestsPerMinute;
        log.info("perMinuteQuota: "+perMinuteQuota);
        RateLimitDefinition definition = RateLimitDefinition.crossDivisionDefinition();
        definition.addQuota(COUNTER_REQUEST_PER_HOUR, new RateLimitDefinition.Quota(playmakerRequestsPerHour, 1, TimeUnit.HOURS));
        definition.addQuota(COUNTER_REQUEST_PER_MINUTE, new RateLimitDefinition.Quota(perMinuteQuota, 1, TimeUnit.MINUTES));
        RateLimitedResourceManager.registerResource(getResourceName(tenant), definition);
        playmakerRequestRegulatorRegistered = true;
    }

    private String getResourceName(String tenant) {
        return PLAYMAKER_REQUEST_REGULATOR + "_" + tenant;
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

    private Boolean isTestTenant(String tenantId) {

        Set<String> TENANTID_PREFIXES = new HashSet<>(Arrays.asList("LETest", "letest",
                "ScoringServiceImplDeploymentTestNG", "RTSBulkScoreWorkflowDeploymentTestNG", "CDLComponentDeploymentTestNG"));

        boolean findMatch = false;

        for (String prefix: TENANTID_PREFIXES) {
            Pattern pattern = Pattern.compile(prefix);
            Matcher matcher = pattern.matcher(tenantId);
            if (matcher.find()) {
                findMatch = true;
                break;
            }
        }

        return findMatch;
    }

}
