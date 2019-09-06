package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "export-field-metadata-defaults", description = "Rest resource for default play launch export fields")
@RestController
@RequestMapping("/export-field-metadata/defaults")
public class ExportFieldMetadataDefaultsResource {

    @Inject
    private ExportFieldMetadataDefaultsService exportFieldMetadataDefaultsService;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Add new default fields")
    @NoCustomerSpace
    public List<ExportFieldMetadataDefaults> createDefaultFields(
            @RequestBody List<ExportFieldMetadataDefaults> defaultExportFields) {
        return exportFieldMetadataDefaultsService.createDefaultExportFields(defaultExportFields);
    }

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get default fields by system name")
    @NoCustomerSpace
    public List<ExportFieldMetadataDefaults> getDefaultFields(
        @RequestParam(value = "systemName", required = true) CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsService.getAllAttributes(systemName);
    }

    @PutMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Update default fields by system name")
    @NoCustomerSpace
    public List<ExportFieldMetadataDefaults> updateDefaultFields(
        @RequestParam(value = "systemName", required = true) CDLExternalSystemName systemName,
        @RequestBody(required = true) List<ExportFieldMetadataDefaults> defaultExportFields) {
        return exportFieldMetadataDefaultsService.updateDefaultFields(systemName, defaultExportFields);
    }

    @DeleteMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Delete default fields")
    @NoCustomerSpace
    public void deleteDefaultFields(
            @RequestParam(value = "systemName", required = true) CDLExternalSystemName systemName) {
        exportFieldMetadataDefaultsService.deleteBySystemName(systemName);
    }
}
