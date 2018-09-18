package com.latticeengines.apps.cdl.controller;


import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks")
public class DataFeedTaskController {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskController.class);

    private final DataFeedTaskManagerService dataFeedTaskManagerService;

    @Inject
    public DataFeedTaskController(DataFeedTaskManagerService dataFeedTaskManagerService) {
        this.dataFeedTaskManagerService = dataFeedTaskManagerService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTaskForVdb(@PathVariable String customerSpace,
                                                                 @RequestParam(value = "source") String source,
                                                                 @RequestParam(value = "feedtype") String feedtype,
                                                                 @RequestParam(value = "entity") String entity,
                                                                 @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        if (vdbLoadTableConfig == null) {
            return ResponseDocument.failedResponse(new RuntimeException("Vdb load table config can't be null!"));
        } else {
            VdbImportConfig vdbImportConfig = new VdbImportConfig();
            vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
            return createDataFeedTask(customerSpace, source, feedtype, entity, vdbImportConfig);
        }
    }

    @RequestMapping(value = "/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTask(@PathVariable String customerSpace,
                                                                 @RequestParam(value = "source") String source,
                                                                 @RequestParam(value = "feedtype") String feedtype,
                                                                 @RequestParam(value = "entity") String entity,
                                                                 @RequestBody CDLImportConfig importConfig) {
        try {
            customerSpace = CustomerSpace.parse(customerSpace).toString();
            entity = BusinessEntity.getByName(entity).name();
            String taskId = dataFeedTaskManagerService.createDataFeedTask(customerSpace, feedtype, entity, source,
                    importConfig);
            return ResponseDocument.successResponse(taskId);
        } catch (Exception e) {
            log.error(String.format("Failed to create data feed task, exception: %s", e.toString()), e);
            return ResponseDocument.failedResponse(e);
        }

    }

    @RequestMapping(value = "/import/{taskIdentifier}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String>  startImportJobForVdb(@PathVariable String customerSpace,
                                                         @PathVariable String taskIdentifier,
                                                         @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        VdbImportConfig vdbImportConfig = new VdbImportConfig();
        vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
        return startImportJob(customerSpace, taskIdentifier, vdbImportConfig);
    }

    @RequestMapping(value = "/import/internal/{taskIdentifier}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String>  startImportJob(@PathVariable String customerSpace,
                                                              @PathVariable String taskIdentifier,
                                                              @RequestBody CDLImportConfig importConfig) {
        try {
            String applicationId = dataFeedTaskManagerService.submitImportJob(customerSpace, taskIdentifier, importConfig);
            return ResponseDocument.successResponse(applicationId);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/s3import", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String>  startImportJobForS3(@PathVariable String customerSpace,
                                                          @RequestBody S3FileToHdfsConfiguration s3FileToHdfsConfiguration) {
        try {
            String applicationId = dataFeedTaskManagerService.submitS3ImportJob(customerSpace,
                    s3FileToHdfsConfiguration);
            return ResponseDocument.successResponse(applicationId);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/reset", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    public ResponseDocument<Boolean> resetImport(@PathVariable String customerSpace,
                            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        if (dataFeedTaskManagerService.resetImport(customerSpace, entity)) {
            return ResponseDocument.successResponse(true);
        } else {
            return ResponseDocument.failedResponse(new RuntimeException("Cannot reset datafeed task."));
        }

    }
}
