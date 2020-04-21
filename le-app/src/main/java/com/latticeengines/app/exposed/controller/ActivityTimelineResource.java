package com.latticeengines.app.exposed.controller;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.DataPage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activityTimeline", description = "REST resource for activity timelines")
@RestController
@RequestMapping("/activity-timeline")
public class ActivityTimelineResource {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineResource.class);

    @Value("${app.timeline.default.period}")
    private String defaultTimelinePeriod;

    @GetMapping(value = "/accounts/{accountId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    @SuppressWarnings("ConstantConditions")
    public DataPage getAccountActivities(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultTimelinePeriod;
        log.info(String.format("Retrieving ActivityData of accountId %s for past %s, ( tenantId: %s )", accountId,
                timelinePeriod, space.getTenantId()));
        return new DataPage();
    }

    @GetMapping(value = "/accounts/{accountId:.+}/contacts/{contactId:.+}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    @SuppressWarnings("ConstantConditions")
    public DataPage getContactActivities(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @PathVariable String contactId, //
            @RequestParam(value = "timeline-period", required = false) String timelinePeriod) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultTimelinePeriod;
        log.info(String.format("Retrieving ActivityData of accountId %s for past %s, ( tenantId: %s )", accountId,
                timelinePeriod, space.getTenantId()));
        return new DataPage();
    }

}
