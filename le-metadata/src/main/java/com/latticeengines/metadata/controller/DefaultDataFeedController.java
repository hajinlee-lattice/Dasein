package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.service.DataFeedService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * This controller assumes operation on the default datafeed in default datacollection
 */
@Api(value = "datafeed", description = "REST resource for default metadata data feed")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeed")
public class DefaultDataFeedController {

    @Autowired
    private DataFeedService datafeedService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed findDataFeedByName(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.getOrCreateDataFeed(customerSpace);
    }

    @RequestMapping(value = "/startexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    public DataFeedExecution startExecution(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.startExecution(customerSpace, "");
    }

    @RequestMapping(value = "/status/{status}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedStatus(@PathVariable String customerSpace, @PathVariable String status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeed(customerSpace, "", status);
    }

    @RequestMapping(value = "/status/{initialDataFeedStatus}/finishexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    public DataFeedExecution finishExecution(@PathVariable String customerSpace, @PathVariable String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.finishExecution(customerSpace, "", initialDataFeedStatus);
    }

    @RequestMapping(value = "/status/{initialDataFeedStatus}/failexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace, @PathVariable String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.failExecution(customerSpace, "", initialDataFeedStatus);
    }

    @RequestMapping(value = "/execution/workflow/{workflowId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace, @PathVariable Long workflowId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.updateExecutionWorkflowId(customerSpace, "", workflowId);
    }


    @RequestMapping(value = "/restartexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "restart data feed execution")
    public DataFeedExecution restartExecution(@PathVariable String customerSpace) {
        return datafeedService.retryLatestExecution(customerSpace, null);
    }

}
