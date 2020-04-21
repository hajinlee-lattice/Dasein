package com.latticeengines.objectapi.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.objectapi.service.ActivityTimelineQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activityTimeline", description = "REST resource to query timeseries data")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/activity")
public class ActivityResource {
    private static final Logger log = LoggerFactory.getLogger(ActivityResource.class);

    @Inject
    private ActivityTimelineQueryService activityTimelineQueryService;

    @PostMapping(value = "/data", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve timeseries activity data")
    public DataPage getActivityData(@PathVariable String customerSpace,
            @RequestParam(value = "version", required = false) DataCollection.Version version,
            @RequestBody ActivityTimelineQuery query) {
        return activityTimelineQueryService.getData(customerSpace, version, query);
    }
}
