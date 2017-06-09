package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

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

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/datafeedtask")
public class DataFeedResource {

    @Autowired
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @RequestMapping(value = "/createtask", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String createDataFeedTask(@RequestParam (value = "source") String source,
                                     @RequestParam (value = "feedtype") String feedtype,
                                     @RequestParam (value = "entity") String entity,
                                     @RequestParam (value = "datafeedname") String datafeedName,
                                     @RequestBody String metadata,
                                     HttpServletRequest request) {
        return JsonUtils.serialize(ImmutableMap.of("task_id",
                dataFeedTaskManagerService.createDataFeedTask(feedtype, entity, source, datafeedName, metadata)));
    }

    @RequestMapping(value = "/import/{taskIdentifier:\\w+\\.\\w+\\.\\w+\\.\\w+}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public String startImportJob(@PathVariable String taskIdentifier, @RequestBody String metadata,
                                 HttpServletRequest request) {
        return JsonUtils.serialize(ImmutableMap.of("application_id",
                dataFeedTaskManagerService.submitImportJob(taskIdentifier, metadata)));
    }

}
