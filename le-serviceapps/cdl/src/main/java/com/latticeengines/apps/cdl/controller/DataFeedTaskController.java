package com.latticeengines.apps.cdl.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks")
public class DataFeedTaskController {

    @Autowired
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @Deprecated
    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String createDataFeedTaskDeprecated(@PathVariable String customerSpace,
            @RequestParam(value = "source") String source, @RequestParam(value = "feedtype") String feedtype,
            @RequestParam(value = "entity") String entity, @RequestBody String metadata) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return JsonUtils.serialize(ImmutableMap.of("task_id",
                dataFeedTaskManagerService.createDataFeedTask(feedtype, entity, source, metadata)));
    }

    @Deprecated
    @RequestMapping(value = "/import/{taskIdentifier:\\w+\\.\\w+\\.\\w+\\.\\w+}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String startImportJobDeprecated(@PathVariable String customerSpace, @PathVariable String taskIdentifier,
            @RequestBody String metadata) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return JsonUtils.serialize(ImmutableMap.of("application_id",
                dataFeedTaskManagerService.submitImportJob(taskIdentifier, metadata)));
    }
}
