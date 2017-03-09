package com.latticeengines.metadata.controller;

import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.metadata.service.VdbImportExtractService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@Api(value = "metadata", description = "REST resource for data tables")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ImportVdbExtractResource {

    @Autowired
    private VdbImportExtractService vdbImportExtractService;

    @RequestMapping(value = "/vdbextract/{identifier:.+}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get vdb import extract record by identifier")
    public VdbImportExtract getVdbImportExtract(@PathVariable String customerSpace,
            @PathVariable String identifier, HttpServletRequest request) {
        return vdbImportExtractService.getVdbImportExtract(customerSpace, identifier);
    }

    @RequestMapping(value = "/vdbextract/update", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update vdb import extract record")
    public Boolean updateVdbImportExtract(@PathVariable String customerSpace,
            @RequestBody VdbImportExtract vdbImportExtract, HttpServletRequest request) {
        return vdbImportExtractService.updateVdbImportExtract(customerSpace, vdbImportExtract);
    }

    @RequestMapping(value = "/vdbextract/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create vdb import extract record")
    public Boolean createVdbImportExtract(@PathVariable String customerSpace,
            @RequestBody VdbImportExtract vdbImportExtract, HttpServletRequest request) {
        return vdbImportExtractService.createVdbImportExtract(customerSpace, vdbImportExtract);
    }
}
