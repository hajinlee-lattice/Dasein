package com.latticeengines.app.exposed.controller;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
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

import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activityTimeline", description = "REST resource for activity timelines")
@RestController
@RequestMapping("/activity-timeline")
public class ActivityTimelineResource {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineResource.class);

    @Inject
    private ActivityTimelineService activityTimelineService;

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Inject
    private BatonService batonService;

    @GetMapping(value = "/accounts/{accountId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve activity timeline data for an account")
    @SuppressWarnings("ConstantConditions")
    public DataPage getAccountActivities(@RequestHeader(HttpHeaders.AUTHORIZATION) String authToken, //
            @PathVariable String accountId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (!batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ACCOUNT360)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Account 360 is not enabled for tenant: " + customerSpace.getTenantId() });
        }
        log.info(String.format("Retrieving activity timeline data of accountId(ID: %s) for %s period, ( tenantId: %s )",
                accountId, StringUtils.isBlank(timelinePeriod) ? "default" : timelinePeriod,
                customerSpace.getTenantId()));
        return activityTimelineService.getAccountActivities(accountId, timelinePeriod, getOrgInfo(authToken));
    }

    @GetMapping(value = "/accounts/{accountId:.+}/contacts/{contactId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve activity timeline data for a contact")
    @SuppressWarnings("ConstantConditions")
    public DataPage getContactActivities(@RequestHeader(HttpHeaders.AUTHORIZATION) String authToken, //
            @PathVariable String accountId, //
            @PathVariable String contactId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (!batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ACCOUNT360)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Account 360 is not enabled for tenant: " + customerSpace.getTenantId() });
        }
        log.info(String.format(
                "Retrieving activity timeline data of contact(Id: %s), accountId(ID: %s) for %s period, ( tenantId: %s )",
                contactId, accountId, StringUtils.isBlank(timelinePeriod) ? "default" : timelinePeriod,
                customerSpace.getTenantId()));
        return activityTimelineService.getContactActivities(accountId, contactId, timelinePeriod,
                getOrgInfo(authToken));
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
