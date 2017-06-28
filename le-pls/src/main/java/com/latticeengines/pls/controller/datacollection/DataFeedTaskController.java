package com.latticeengines.pls.controller.datacollection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.pls.service.DataFeedTaskManagerService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Deprecated
@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "")
public class DataFeedTaskController {

    @Autowired
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @Deprecated
    @RequestMapping(value = "/datafeedtask/createtask", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String createDataFeedTaskDeprecated(@RequestParam (value = "source") String source,
                                     @RequestParam (value = "feedtype") String feedtype,
                                     @RequestParam (value = "entity") String entity,
                                     @RequestParam (value = "datafeedname") String datafeedName,
                                     @RequestBody String metadata) {
        return JsonUtils.serialize(ImmutableMap.of("task_id",
                dataFeedTaskManagerService.createDataFeedTask(feedtype, entity, source, datafeedName, metadata)));
    }

    @Deprecated
    @RequestMapping(value = "/datafeedtask/import/{taskIdentifier}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String startImportJobDeprecated(@PathVariable String taskIdentifier,
                               @RequestParam (value = "source", required = false, defaultValue = "VisiDB") String source,
                               @RequestBody String metadata) {
        return JsonUtils.serialize(ImmutableMap.of("application_id",
                dataFeedTaskManagerService.submitImportJob(taskIdentifier, source, metadata)));
    }
}
