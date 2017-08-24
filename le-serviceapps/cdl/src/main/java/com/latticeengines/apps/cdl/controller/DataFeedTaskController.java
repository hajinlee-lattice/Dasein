package com.latticeengines.apps.cdl.controller;


import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks")
public class DataFeedTaskController {

    private final DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    public DataFeedTaskController(DataFeedTaskManagerService dataFeedTaskManagerService) {
        this.dataFeedTaskManagerService = dataFeedTaskManagerService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTaskDeprecated(@PathVariable String customerSpace,
                                                                 @RequestParam(value = "source") String source,
                                                                 @RequestParam(value = "feedtype") String feedtype,
                                                                 @RequestParam(value = "entity") String entity,
                                                                 @RequestBody String metadata) {
        try {
            customerSpace = CustomerSpace.parse(customerSpace).toString();
            String taskId = dataFeedTaskManagerService.createDataFeedTask(customerSpace, feedtype, entity, source,
                    metadata);
            return ResponseDocument.successResponse(taskId);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }

    }

    @RequestMapping(value = "/import/{taskIdentifier}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String>  startImportJobDeprecated(@PathVariable String customerSpace,
                                                         @PathVariable String taskIdentifier,
                                                         @RequestBody String metadata) {
        try {
            String applicationId = dataFeedTaskManagerService.submitImportJob(customerSpace, taskIdentifier, metadata);
            return ResponseDocument.successResponse(applicationId);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }
}
