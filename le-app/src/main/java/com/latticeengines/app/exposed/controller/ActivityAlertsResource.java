package com.latticeengines.app.exposed.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.ActivityAlertsService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ActivityAlerts")
@RestController
@RequestMapping("/activity-alerts")
public class ActivityAlertsResource {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineResource.class);

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private ActivityAlertsService activityAlertsService;

    @GetMapping("/config")
    @ResponseBody
    @ApiOperation(value = "Retrieve configuration of activity alerts an tenant")
    @SuppressWarnings("ConstantConditions")
    public List<ActivityAlertsConfig> getActivityAlertsConfiguration(
            @RequestHeader(HttpHeaders.AUTHORIZATION) String authToken) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (!batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ACCOUNT360)) {
            throw new LedpException(LedpCode.LEDP_32002, new String[] { "Account 360", customerSpace.getTenantId() });
        }
        return activityStoreProxy
                .getActivityAlertsConfiguration(CustomerSpace.shortenCustomerSpace(customerSpace.toString()));
    }

    @GetMapping("/accounts/{accountId:.+}")
    @ResponseBody
    @ApiOperation(value = "Retrieve activity alerts for the given Account")
    @SuppressWarnings("ConstantConditions")
    public DataPage getActivityAlertsByAccount(@RequestHeader(HttpHeaders.AUTHORIZATION) String authToken,
            @PathVariable String accountId, //
            @RequestParam(value = "category") String categoryStr,
            @RequestParam(value = "max", required = false, defaultValue = "3") int max) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (!batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ACCOUNT360)) {
            throw new LedpException(LedpCode.LEDP_32002, new String[] { "Account 360", customerSpace.getTenantId() });
        }
        if (!AlertCategory.contains(categoryStr)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format("Invalid value for parameter 'category' %s , Tenant=%s", categoryStr,
                            customerSpace.getTenantId()) });
        }

        try (PerformanceTimer timer = new PerformanceTimer(
                "AccountAlertsLookup: Retrieved AccountAlert data for accountId=" + accountId + " category="
                        + categoryStr + " | Tenant=" + customerSpace.getTenantId())) {
            return activityAlertsService.findActivityAlertsByAccountAndCategory(customerSpace.getTenantId(), accountId,
                    AlertCategory.valueOf(categoryStr.toUpperCase()), max, getOrgInfo(authToken));
        }

    }

    private Map<String, String> getOrgInfo(String token) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            return oauth2RestApiProxy.getOrgInfoFromOAuthRequest(token);
        } catch (Exception e) {
            log.warn("Failed to find orginfo from the authentication token for tenant " + customerSpace);
        }
        return null;
    }
}
