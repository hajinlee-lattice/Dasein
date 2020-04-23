package com.latticeengines.app.exposed.controller;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

    @GetMapping(value = "/accounts/{accountId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve activity timeline data for an account")
    @SuppressWarnings("ConstantConditions")
    public DataPage getAccountActivities(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        log.info(String.format("Retrieving activity timeline data of accountId(ID: %s) for %s period, ( tenantId: %s )",
                accountId, StringUtils.isBlank(timelinePeriod) ? "default" : timelinePeriod, space.getTenantId()));
        return activityTimelineService.getAccountActivities(accountId, timelinePeriod, getOrgInfo(requestEntity));
    }

    @GetMapping(value = "/accounts/{accountId:.+}/contacts/{contactId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve activity timeline data for a contact")
    @SuppressWarnings("ConstantConditions")
    public DataPage getContactActivities(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @PathVariable String contactId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        log.info(String.format(
                "Retrieving activity timeline data of contact(Id: %s), accountId(ID: %s) for %s period, ( tenantId: %s )",
                contactId, accountId, StringUtils.isBlank(timelinePeriod) ? "default" : timelinePeriod,
                space.getTenantId()));
        return activityTimelineService.getContactActivities(accountId, contactId, timelinePeriod,
                getOrgInfo(requestEntity));
    }

    private Map<String, String> getOrgInfo(RequestEntity<String> requestEntity) {
        return oauth2RestApiProxy.getOrgInfoFromOAuthRequest(requestEntity);
    }
}
