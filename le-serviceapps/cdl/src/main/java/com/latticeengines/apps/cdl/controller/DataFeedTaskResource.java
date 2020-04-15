package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data feed task")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datafeed/tasks")
public class DataFeedTaskResource {

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createOrUpdateDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity, @PathVariable String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createOrUpdateDataFeedTask(customerSpace, source, dataFeedType, entity, tableName);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void updateDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask,
                                   @RequestParam(required = false) Boolean updateTaskOnly) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, Boolean.TRUE.equals(updateTaskOnly));
    }

    @RequestMapping(value = "/s3status/{source}/{dataFeedType}/{status}", method = RequestMethod.PUT,
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task s3 import status")
    public void updateS3ImportStatus(@PathVariable String customerSpace, @PathVariable String source,
                                   @PathVariable String dataFeedType,
                                   @PathVariable DataFeedTask.S3ImportStatus status) {
        dataFeedTaskService.updateS3ImportStatus(customerSpace, source, dataFeedType, status);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType, entity);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
                                        @PathVariable String dataFeedType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType);
    }

    @RequestMapping(value = "/{taskId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String taskId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, taskId);
    }

    @GetMapping("/source/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "Get data feed task by source")
    public DataFeedTask getDataFeedTaskBySource(@PathVariable String customerSpace, @PathVariable String sourceId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskBySource(customerSpace, sourceId);
    }

    @RequestMapping(value = "/{entity}/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(@PathVariable String customerSpace,
            @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskWithSameEntity(customerSpace, entity);
    }

    @RequestMapping(value = "/{entity}/{excludeSource}/{excludeFeedType}/list", method = RequestMethod.GET, headers = "Accept" +
            "=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskWithSameEntityExcludeOne(@PathVariable String customerSpace,
                                                                      @PathVariable String entity,
                                                                      @PathVariable String excludeSource,
                                                                      @PathVariable String excludeFeedType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskWithSameEntityExcludeOne(customerSpace, entity, excludeSource,
                excludeFeedType);
    }

    @RequestMapping(value = "/byuniqueids", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskByUniqueIds(@PathVariable String customerSpace,
                                                            @RequestBody List<String> uniqueIds) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskByUniqueIds(customerSpace, uniqueIds);
    }

    @RequestMapping(value = "/{taskId}/registerextract/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public List<String> registerExtract(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody Extract extract) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.registerExtract(customerSpace, taskId, tableName, extract);
    }

    @RequestMapping(value = "/{taskId}/registerextracts/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public List<String> registerExtracts(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody List<Extract> extracts) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.registerExtracts(customerSpace, taskId, tableName, extracts);
    }

    @RequestMapping(value = "/{taskId}/addtabletoqueue/{tableName}", method = RequestMethod.PUT, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add table to data feed task table queue")
    public void addTableToQueue(@PathVariable String customerSpace, @PathVariable String taskId,
                                @PathVariable String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.addTableToQueue(customerSpace, taskId, tableName);
    }

    @PutMapping("/{taskId}/addtablestoqueue")
    @ResponseBody
    @ApiOperation(value = "Add tables to data feed task table queue")
    public void addTablesToQueue(@PathVariable String customerSpace, @PathVariable String taskId,
                                 @RequestBody List<String> tables) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.addTablesToQueue(customerSpace, taskId, tables);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}/unconsolidatedextracts", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get unconsolidated extracts in queue")
    public List<Extract> getExtractsPendingInQueue(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getExtractsPendingInQueue(customerSpace, source, dataFeedType, entity);
    }

    @RequestMapping(value = "/{entity}/getTables", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task template tables")
    public List<Table> getTemplateTables(@PathVariable String customerSpace,
                                         @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateTables(customerSpace, entity);
    }

    @RequestMapping(value = "/{taskId}/getSystem", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task template tables")
    public S3ImportSystem getSystemFromTaskId(@PathVariable String customerSpace,
                                              @PathVariable String taskId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getImportSystemByTaskId(customerSpace, taskId);
    }

    @GetMapping(value = "/{entity}/getTemplatesBySystemPriority")
    @ResponseBody
    @ApiOperation(value = "Get templates ordered by system priority")
    public List<String> getTemplatesBySystemPriority(@PathVariable String customerSpace, @PathVariable String entity,
                                                     @RequestParam(required = false) Boolean highestFirst) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplatesBySystemPriority(customerSpace, entity, Boolean.TRUE.equals(highestFirst));
    }

    @PutMapping(value = "/{pid}/deleted/{deleted}")
    @ApiOperation(value = "Set data feed task deleted status")
    public void setDataFeedTaskDeleteStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                                            @PathVariable Boolean deleted) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.setDataFeedTaskDelete(customerSpace, pid, deleted);
    }

    @PutMapping(value = "/{pid}/S3Import/status/{status}")
    @ApiOperation(value = "Set data feed task s3 import status")
    public void setDataFeedTaskS3ImportStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                                            @PathVariable DataFeedTask.S3ImportStatus status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.setDataFeedTaskS3ImportStatus(customerSpace, pid, status);
    }

    @GetMapping(value = "/{taskUniqueId}/templateName")
    @ResponseBody
    @ApiOperation(value = "Get template name by task unique id")
    public String getTemplateName(@PathVariable String customerSpace, @PathVariable String taskUniqueId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateName(customerSpace, taskUniqueId);
    }

    @GetMapping(value = "/templateToSystemMap")
    @ResponseBody
    @ApiOperation(value = "Get template to import system Map")
    public Map<String, String> getTemplateName(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateToSystemMap(customerSpace);
    }
}
