package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataOperationService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DataOperationRequest;
import com.latticeengines.domain.exposed.metadata.DataOperation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dataoperation", description = "REST resource for data operation")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dataoperation")
public class DataOperationResource {
    private static final Logger log = LoggerFactory.getLogger(DataOperationResource.class);

    @Inject
    DataOperationService dataOperationService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "create data operation")
    public String create(@PathVariable String customerSpace,
            @RequestParam("operationType") DataOperation.OperationType operationType,
            @RequestBody DataOperationConfiguration dataOperationConfiguration) {
        try {
            return dataOperationService.createDataOperation(customerSpace, operationType, dataOperationConfiguration);
        } catch (Exception e) {
            log.error("error:", e);
            return e.getMessage();
        }
    }

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "find data operation by drop path")
    public DataOperation findDataOperationByDropPath(@PathVariable String customerSpace,
                       @RequestParam("dropPath") String dropPath) {
        return dataOperationService.findDataOperationByDropPath(customerSpace, dropPath);
    }

    @PostMapping("/submitJob")
    @ResponseBody
    @ApiOperation(value = "submit data operation job")
    public ResponseDocument<String> submitJob(@PathVariable String customerSpace,
                                      @RequestBody DataOperationRequest dataOperationRequest) {
        try {
            return ResponseDocument.successResponse(
                    dataOperationService.submitJob(customerSpace, dataOperationRequest).toString());
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }
}
