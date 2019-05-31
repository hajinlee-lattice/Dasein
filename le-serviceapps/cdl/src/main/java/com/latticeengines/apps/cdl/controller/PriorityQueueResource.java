package com.latticeengines.apps.cdl.controller;

import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.util.PriorityQueueUtils;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "priorityQueue", description = "Rest resource for get priority queue")
@RestController
@RequestMapping("/priorityqueue")
public class PriorityQueueResource {

    @RequestMapping(value = "/getHigh", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "getHighPriorityQueue")
    @NoMetricsLog
    @NoCustomerSpace
    public List<String> getHighPriorityQueue() {
        return PriorityQueueUtils.getAllMemberWithSortFromHighPriorityQueue();
    }

    @RequestMapping(value = "/getLow", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "getHighPriorityQueue")
    @NoMetricsLog
    @NoCustomerSpace
    public List<String> getLowPriorityQueue() {
        return PriorityQueueUtils.getAllMemberWithSortFromLowPriorityQueue();
    }

    @RequestMapping(value = "/removeActivity", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "remove Activity From Queue")
    public void removeActivityFromQueue(@RequestParam String tenantName) {
        PriorityQueueUtils.removeActivityFromQueue(tenantName);
    }

}
