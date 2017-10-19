package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.metadata.service.DataFeedService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * This controller assumes operation on the default datafeed in default
 * datacollection
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

    @RequestMapping(value = "/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed getDefaultDataFeed(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.getDefaultDataFeed(customerSpace);
    }

    @RequestMapping(value = "/drainingstatus/{drainingStatus}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedDrainingStatus(@PathVariable String customerSpace, @PathVariable String drainingStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeedDrainingStatus(customerSpace, drainingStatus);
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
    public DataFeedExecution finishExecution(@PathVariable String customerSpace,
            @PathVariable String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.finishExecution(customerSpace, "", initialDataFeedStatus);
    }

    @RequestMapping(value = "/status/{initialDataFeedStatus}/failexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace,
            @PathVariable String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.failExecution(customerSpace, "", initialDataFeedStatus);
    }

    @RequestMapping(value = "/execution/workflow/{workflowId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace,
            @PathVariable Long workflowId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.updateExecutionWorkflowId(customerSpace, "", workflowId);
    }

    @RequestMapping(value = "/restartexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "restart data feed execution")
    public DataFeedExecution restartExecution(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.retryLatestExecution(customerSpace, null);
    }

    @RequestMapping(value = "/startprofile", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "start data feed profile")
    public DataFeedProfile startProfile(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.startProfile(customerSpace, "");
    }

    @RequestMapping(value = "/status/{initialDataFeedStatus}/finishprofile", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "finish data feed profile")
    public DataFeed finishProfile(@PathVariable String customerSpace, @PathVariable String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.finishProfile(customerSpace, "", initialDataFeedStatus);
    }

    @RequestMapping(value = "/profile/workflow/{workflowId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed profile")
    public DataFeedProfile updateProfileWorkflowId(@PathVariable String customerSpace, @PathVariable Long workflowId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.updateProfileWorkflowId(customerSpace, "", workflowId);
    }

    @RequestMapping(value = "/resetimport", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset the pending import data for this data feed")
    public void resetImport(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.resetImport(customerSpace, "");
    }
}
