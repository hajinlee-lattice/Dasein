package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DataFeedTaskTemplateService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
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
    private DataFeedTaskTemplateService dataFeedTaskTemplateService;

    @Inject
    public DataFeedTaskController(DataFeedTaskManagerService dataFeedTaskManagerService) {
        this.dataFeedTaskManagerService = dataFeedTaskManagerService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTaskForVdb(@PathVariable String customerSpace,
            @RequestParam(value = "source") String source, @RequestParam(value = "feedtype") String feedtype,
            @RequestParam(value = "entity") String entity, @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        if (vdbLoadTableConfig == null) {
            return ResponseDocument.failedResponse(new RuntimeException("Vdb load table config can't be null!"));
        } else {
            VdbImportConfig vdbImportConfig = new VdbImportConfig();
            vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
            return createDataFeedTask(customerSpace, source, feedtype, entity, "", "", false, "", vdbImportConfig);
        }
    }

    @RequestMapping(value = "/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> createDataFeedTask(@PathVariable String customerSpace,
            @RequestParam(value = "source") String source, @RequestParam(value = "feedtype") String feedtype,
            @RequestParam(value = "entity") String entity,
            @RequestParam(value = "subType", required = false) String subType,
            @RequestParam(value = "displayName", required = false) String displayName,
            @RequestParam(value = "sendEmail", required = false, defaultValue = "false") boolean sendEmail,
            @RequestParam(value = "user", required = false) String user, @RequestBody CDLImportConfig importConfig) {
        try {
            customerSpace = CustomerSpace.parse(customerSpace).toString();
            entity = BusinessEntity.getByName(entity).name();
            String taskId = dataFeedTaskManagerService.createDataFeedTask(customerSpace, feedtype, entity, source,
                    subType, displayName != null ? java.net.URLDecoder.decode(displayName, "UTF-8") : null, sendEmail,
                    user, importConfig);
            return ResponseDocument.successResponse(taskId);
        } catch (Exception e) {
            log.error(String.format("Failed to create data feed task, exception: %s", e.toString()), e);
            return ResponseDocument.failedResponse(e);
        }

    }

    @RequestMapping(value = "/import/{taskIdentifier}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJobForVdb(@PathVariable String customerSpace,
            @PathVariable String taskIdentifier, @RequestBody VdbLoadTableConfig vdbLoadTableConfig) {
        VdbImportConfig vdbImportConfig = new VdbImportConfig();
        vdbImportConfig.setVdbLoadTableConfig(vdbLoadTableConfig);
        return startImportJob(customerSpace, taskIdentifier, false, vdbImportConfig);
    }

    @RequestMapping(value = "/import/internal/{taskIdentifier}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJob(@PathVariable String customerSpace,
            @PathVariable String taskIdentifier,
            @RequestParam(value = "onlyData", required = false, defaultValue = "false") boolean onlyData,
            @RequestBody CDLImportConfig importConfig) {
        try {
            if (onlyData) {
                String applicationId = dataFeedTaskManagerService.submitDataOnlyImportJob(customerSpace, taskIdentifier,
                        (CSVImportConfig) importConfig);
                return ResponseDocument.successResponse(applicationId);
            } else {
                String applicationId = dataFeedTaskManagerService.submitImportJob(customerSpace, taskIdentifier,
                        importConfig);
                return ResponseDocument.successResponse(applicationId);
            }
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/s3import", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a data feed task")
    @NoCustomerSpace
    public ResponseDocument<String> startImportJobForS3(@PathVariable String customerSpace,
            @RequestBody S3FileToHdfsConfiguration s3FileToHdfsConfiguration) {
        try {
            String applicationId = dataFeedTaskManagerService.submitS3ImportJob(customerSpace,
                    s3FileToHdfsConfiguration);
            return ResponseDocument.successResponse(applicationId);
        } catch (Exception e) {
            log.info("Failed to start s3 import", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/reset", method = RequestMethod.POST, headers = "Accept=application/json")
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

    @PostMapping(value = "/setup/webvisit")
    @ResponseBody
    @ApiOperation(value = "Create a WebVist template")
    public ResponseDocument<Boolean> createWebVisitTemplate(@PathVariable String customerSpace,
                                                            @RequestBody SimpleTemplateMetadata simpleTemplateMetadata) {
        try {
            return ResponseDocument.successResponse(
                    dataFeedTaskTemplateService.setupWebVisitTemplate(customerSpace, simpleTemplateMetadata));
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/diagnostic/{taskIdentifier}")
    @ResponseBody
    @ApiOperation(value = "Template diagnostic")
    public ResponseDocument<ImportTemplateDiagnostic> templateDiagnostic(@PathVariable String customerSpace,
                                                                         @PathVariable String taskIdentifier) {
        return ResponseDocument.successResponse(dataFeedTaskManagerService.diagnostic(customerSpace, taskIdentifier));
    }
}
