package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.SourceInfo;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data feed task")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeed/tasks")
public class DataFeedTaskResource {

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);
    }

    @PostMapping("/{source}/{dataFeedType}/{entity}/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createOrUpdateDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity, @PathVariable String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createOrUpdateDataFeedTask(customerSpace, source, dataFeedType, entity, tableName);
    }

    @PutMapping("")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void updateDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask,
                                   @RequestParam(required = false) Boolean updateTaskOnly) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, Boolean.TRUE.equals(updateTaskOnly));
    }

    @PutMapping("/s3status/{source}/{dataFeedType}/{status}")
    @ResponseBody
    @ApiOperation(value = "Update data feed task s3 import status")
    public void updateS3ImportStatus(@PathVariable String customerSpace, @PathVariable String source,
                                   @PathVariable String dataFeedType,
                                   @PathVariable DataFeedTask.S3ImportStatus status) {
        dataFeedTaskService.updateS3ImportStatus(customerSpace, source, dataFeedType, status);
    }

    @GetMapping("/{source}/{dataFeedType}/{entity}")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType, entity);
    }

    @GetMapping("/{source}/{dataFeedType}")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
                                        @PathVariable String dataFeedType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType);
    }

    @GetMapping("/{taskId}")
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

    @GetMapping("/onlySource/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "Get data feed task by source")
    public SourceInfo getSourceBySource(@PathVariable String customerSpace, @PathVariable String sourceId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getSourceBySourceId(customerSpace, sourceId);
    }

    @GetMapping("/systemPid/{systemPid}")
    @ResponseBody
    @ApiOperation(value = "Get Source list by system pid")
    public List<SourceInfo> getSourcesBySystemPid(@PathVariable String customerSpace, @PathVariable Long systemPid,
                                                  @RequestParam(defaultValue = "0") int pageIndex,
                                                  @RequestParam(defaultValue = "20") int pageSize) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getSourcesBySystemPid(customerSpace, systemPid, pageIndex, pageSize);
    }

    @GetMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Get Source list by projectId")
    public List<SourceInfo> getSourcesByProjectId(@PathVariable String customerSpace, @PathVariable String projectId,
                                                  @RequestParam(defaultValue = "0") int pageIndex,
                                                  @RequestParam(defaultValue = "20") int pageSize) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getSourcesByProjectId(customerSpace, projectId, pageIndex, pageSize);
    }

    @GetMapping("/count/systemPid/{systemPid}")
    @ResponseBody
    @ApiOperation(value = "Get total Source count by system pid")
    public Long countSourcesBySystemPid(@PathVariable String customerSpace, @PathVariable Long systemPid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.countSourcesBySystemPid(customerSpace, systemPid);
    }

    @GetMapping("/count/projectId/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Get total Source count by projectId")
    public Long countSourcesByProjectId(@PathVariable String customerSpace, @PathVariable String projectId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.countSourcesByProjectId(customerSpace, projectId);
    }

    @GetMapping("/{entity}/list")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(@PathVariable String customerSpace,
            @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskWithSameEntity(customerSpace, entity);
    }

    @GetMapping("/{entity}/{excludeSource}/{excludeFeedType}/list")
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

    @PostMapping("/byuniqueids")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskByUniqueIds(@PathVariable String customerSpace,
                                                            @RequestBody List<String> uniqueIds) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskByUniqueIds(customerSpace, uniqueIds);
    }

    @PostMapping("/{taskId}/registerextract/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public List<String> registerExtract(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody Extract extract) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.registerExtract(customerSpace, taskId, tableName, extract);
    }

    @PostMapping("/{taskId}/registerextracts/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public List<String> registerExtracts(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody List<Extract> extracts) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.registerExtracts(customerSpace, taskId, tableName, extracts);
    }

    @PutMapping("/{taskId}/addtabletoqueue/{tableName}")
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

    @GetMapping("/{source}/{dataFeedType}/{entity}/unconsolidatedextracts")
    @ResponseBody
    @ApiOperation(value = "Get unconsolidated extracts in queue")
    public List<Extract> getExtractsPendingInQueue(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getExtractsPendingInQueue(customerSpace, source, dataFeedType, entity);
    }

    @GetMapping("/{entity}/getTables")
    @ResponseBody
    @ApiOperation(value = "Get data feed task template tables")
    public List<Table> getTemplateTables(@PathVariable String customerSpace,
                                         @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateTables(customerSpace, entity);
    }

    @GetMapping("/{taskId}/getSystem")
    @ResponseBody
    @ApiOperation(value = "Get data feed task template tables")
    public S3ImportSystem getSystemFromTaskId(@PathVariable String customerSpace,
                                              @PathVariable String taskId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getImportSystemByTaskId(customerSpace, taskId);
    }

    @GetMapping("/{entity}/getTemplatesBySystemPriority")
    @ResponseBody
    @ApiOperation(value = "Get templates ordered by system priority")
    public List<String> getTemplatesBySystemPriority(@PathVariable String customerSpace, @PathVariable String entity,
                                                     @RequestParam(required = false) Boolean highestFirst) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplatesBySystemPriority(customerSpace, entity, Boolean.TRUE.equals(highestFirst));
    }

    @PutMapping("/{pid}/deleted/{deleted}")
    @ApiOperation(value = "Set data feed task deleted status")
    public void setDataFeedTaskDeleteStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                                            @PathVariable Boolean deleted) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.setDataFeedTaskDelete(customerSpace, pid, deleted);
    }

    @PutMapping("/{pid}/S3Import/status/{status}")
    @ApiOperation(value = "Set data feed task s3 import status")
    public void setDataFeedTaskS3ImportStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                                            @PathVariable DataFeedTask.S3ImportStatus status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.setDataFeedTaskS3ImportStatus(customerSpace, pid, status);
    }

    @GetMapping("/{taskUniqueId}/templateName")
    @ResponseBody
    @ApiOperation(value = "Get template name by task unique id")
    public String getTemplateName(@PathVariable String customerSpace, @PathVariable String taskUniqueId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateName(customerSpace, taskUniqueId);
    }

    @GetMapping("/templateToSystemMap")
    @ResponseBody
    @ApiOperation(value = "Get template to import system Map")
    public Map<String, String> getTemplateToSystemMap(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateToSystemMap(customerSpace);
    }

    @GetMapping("/templateToSystemObjectMap")
    @ResponseBody
    @ApiOperation(value = "Get template to import system Map")
    public Map<String, S3ImportSystem> getTemplateToSystemObjMap(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateToSystemObjectMap(customerSpace);
    }

    @GetMapping("/templateToSystemTypeMap")
    @ResponseBody
    @ApiOperation(value = "Get template to import systemType Map")
    public Map<String, S3ImportSystem.SystemType> getTemplateToSystemTypeMap(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateToSystemTypeMap(customerSpace);
    }

    @GetMapping("/templateToDataFeedTaskMap")
    @ResponseBody
    @ApiOperation(value = "Get template to DataFeedTask Map")
    public Map<String, DataFeedTask> getTemplateToDataFeedTaskMap(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getTemplateToDataFeedTaskMap(customerSpace);
    }

    @GetMapping("/summaries/{source}")
    @ResponseBody
    @ApiOperation(value = "Get DataFeedTask Summaries for Source")
    public List<DataFeedTaskSummary> getDataFeedTaskSummaries(@PathVariable String customerSpace,
                                                              @PathVariable String source) {
        return dataFeedTaskService.getSummaryBySourceAndDataFeed(customerSpace, source);
    }

    @GetMapping("/exists/{source}/{dataFeedType}")
    @ResponseBody
    @ApiOperation(value = "Get DataFeedTask Summaries for Source")
    public Boolean existsDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
                                      @PathVariable String dataFeedType) {
        return dataFeedTaskService.existsBySourceAndFeedType(customerSpace, source, dataFeedType);
    }

    @DeleteMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Delete DataFeedTask by projectId")
    public void deleteDataFeedTaskByProjectId(@PathVariable String customerSpace, @PathVariable String projectId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.deleteDataFeedTaskByProjectId(customerSpace, projectId);
    }
}
