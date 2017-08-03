package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataCollectionManagerService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "Controller of data collection operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection")
public class DataCollectionController {

    private final DataCollectionManagerService collectionMgrSvc;

    @Inject
    public DataCollectionController(DataCollectionManagerService collectionMgrSvc) {
        this.collectionMgrSvc = collectionMgrSvc;
    }

    @RequestMapping(value = "/reset", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Reset the full data collection or an business entity")
    public ResponseDocument<String> reset(@PathVariable String customerSpace,
           @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        String customerSpaceString = CustomerSpace.parse(customerSpace).toString();
        Boolean status;
        if (entity == null) {
            status = collectionMgrSvc.resetAll(customerSpaceString);
        } else {
            status = collectionMgrSvc.resetEntity(customerSpaceString, entity);
        }
        if (status = true) {
            return ResponseDocument.successResponse("Success");
        } else {
            return ResponseDocument.failedResponse(new RuntimeException("Failed to reset"));
        }
    }
}
