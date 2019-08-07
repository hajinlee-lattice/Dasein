package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ConvertBatchStoreInfoService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "convert_batchstore_info", description = "REST resource for ConvertBatchStore info")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/convertbatchstoreinfo")
public class ConvertBatchStoreInfoResource {

    @Inject
    private ConvertBatchStoreInfoService convertBatchStoreInfoService;

    @PostMapping("/create")
    @ResponseBody
    @ApiOperation(value = "Create a ConvertBatchStoreInfo record")
    public ConvertBatchStoreInfo createConvertBatchStoreInfo(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return convertBatchStoreInfoService.create(customerSpace);
    }

    @GetMapping("/get/{pid}")
    @ResponseBody
    @ApiOperation(value = "Get a ConvertBatchStoreInfo record by pid")
    public ConvertBatchStoreInfo getConvertBatchStoreInfo(@PathVariable String customerSpace, @PathVariable Long pid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return convertBatchStoreInfoService.getByPid(customerSpace, pid);
    }

    @PutMapping("/update/{pid}/detail")
    @ResponseBody
    @ApiOperation(value = "Update a ConvertBatchStoreInfo record detail")
    public void updateConvertBatchStoreInfoDetail(@PathVariable String customerSpace, @PathVariable Long pid,
                                                  @RequestBody ConvertBatchStoreDetail detail) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        convertBatchStoreInfoService.updateDetails(customerSpace, pid, detail);
    }
}
