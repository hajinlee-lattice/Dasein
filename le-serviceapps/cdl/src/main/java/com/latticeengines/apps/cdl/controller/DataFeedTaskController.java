package com.latticeengines.apps.cdl.controller;

import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks")
public class DataFeedTaskController extends InternalResourceBase {

    private final DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    public DataFeedTaskController(DataFeedTaskManagerService dataFeedTaskManagerService) {
        this.dataFeedTaskManagerService = dataFeedTaskManagerService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public Map<String, String> createDataFeedTaskDeprecated(HttpServletRequest request, @PathVariable String customerSpace,
                                                            @RequestParam(value = "source") String source,
                                                            @RequestParam(value = "feedtype") String feedtype,
                                                            @RequestParam(value = "entity") String entity,
                                                            @RequestBody String metadata) {
        checkHeader(request);
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return ImmutableMap.of("task_id",
                dataFeedTaskManagerService.createDataFeedTask(customerSpace, feedtype, entity, source, metadata));
    }

    @RequestMapping(value = "/import/{taskIdentifier}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public Map<String, String>  startImportJobDeprecated(HttpServletRequest request,
                                                         @PathVariable String customerSpace,
                                                         @PathVariable String taskIdentifier,
                                                         @RequestBody String metadata) {
        checkHeader(request);
        return ImmutableMap.of("application_id",
                dataFeedTaskManagerService.submitImportJob(customerSpace, taskIdentifier, metadata));
    }
}
