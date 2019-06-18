package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.SchedulingPAService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "priorityQueue", description = "Rest resource for get priority queue")
@RestController
@RequestMapping("/schedulingPAQueue")
public class SchedulingPAQueueResource {

    @Inject
    private SchedulingPAService schedulingPAService;

    @RequestMapping(value = "/getQueueInfo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "getQueueInfo")
    @NoMetricsLog
    @NoCustomerSpace
    public String getQueueInfo() {
        return schedulingPAService.showQueue();
    }

    @RequestMapping(value = "/getPosition", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "getPosition")
    @NoMetricsLog
    @NoCustomerSpace
    public String getPosition(@RequestParam String tenantName) {
        return schedulingPAService.getPositionFromQueue(tenantName);
    }

}
