package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.TimeLineService;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "timelines", description = "REST resource for timeline management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/timelines")
public class TimeLineResource {

    @Inject
    private TimeLineService timeLineService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation("Get all timelines under current tenant")
    public List<TimeLine> getTimeLines(@PathVariable(value = "customerSpace") String customerSpace) {
        return timeLineService.findByTenant(customerSpace);
    }

    @GetMapping("/entity/{entity}")
    @ResponseBody
    @ApiOperation("Get a timeline by Entity from current tenant")
    public TimeLine getTimeLines(@PathVariable(value = "customerSpace") String customerSpace,
            @PathVariable BusinessEntity entity) {
        return timeLineService.findByTenantAndEntity(customerSpace, entity);
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation("Create or update a timeline under current tenant")
    public TimeLine createTimeLine( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody TimeLine timeLine) {
        return timeLineService.createOrUpdateTimeLine(customerSpace, timeLine);
    }

    @PostMapping("/createDefault")
    @ResponseBody
    @ApiOperation("create default Account360/Contact360 timeline under current tenant, must be unique.")
    public Boolean createDefaultTimeLine(@PathVariable(value = "customerSpace") String customerSpace) {
        timeLineService.createDefaultTimeLine(customerSpace);
        return true;
    }
}
