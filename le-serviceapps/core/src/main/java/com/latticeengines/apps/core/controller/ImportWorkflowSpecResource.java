package com.latticeengines.apps.core.controller;


import java.io.IOException;
import java.util.List;

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

import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ImportWorkflowSpec", description = "REST resource for Import Workflow Spec operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/importworkflowspec")
public class ImportWorkflowSpecResource {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecResource.class);

    @Inject
    private ImportWorkflowSpecService importWorkflowSpecService;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation("get import workflow spec")
    public ImportWorkflowSpec getImportWorkflowSpec(
            @PathVariable String customerSpace, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject) {
        ImportWorkflowSpec spec;
        try {
            spec = importWorkflowSpecService.loadSpecFromS3(systemType, systemObject);
        } catch (IOException e) {
            log.error(String.format(
                    "ImportWorkflowSpecService failed to return Spec for system type %s and system object %s.\n" +
                            "Error was: %s", systemType, systemObject, e.toString()));
            return null;
        }
        return spec;
    }

    // tableName is the optional user provided name for the table.
    // writeAll should be set true to add all FieldDefinitions from the record to the Table, even those that do not
    // have a columnName, meaning they were not mapped in the imported file.
    @PostMapping(value = "/table")
    @ResponseBody
    @ApiOperation("generate table from field definition record")
    public Table generateTable(
            @PathVariable String customerSpace, //
            @RequestParam(value = "tableName", required = false) String tableName, //
            @RequestParam(value = "writeAll", required = false, defaultValue = "false") boolean writeAll, //
            @RequestBody FieldDefinitionsRecord record) {
        Table table;
        try {
            table = importWorkflowSpecService.tableFromRecord(tableName, writeAll, record);
        } catch (Exception e) {
            log.error(String.format(
                    "Could not create Attribute Table named %s from Spec for system type %s and system object %s",
                    tableName, record.getSystemType(), record.getSystemObject()));
            return null;
        }
        return table;
    }

    @GetMapping(value = "/list")
    @ResponseBody
    @ApiOperation("get workflow spec with same object excluding one type")
    public List<ImportWorkflowSpec> getImportWorkflowSpecWithSameObjectExcludeType(
            @PathVariable String customerSpace, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject) {
        List<ImportWorkflowSpec> specs;
        try {
            specs = importWorkflowSpecService.loadSpecWithSameObjectExcludeTypeFromS3(systemType, systemObject);
        } catch (Exception e) {
            log.error(String.format(
                    "ImportWorkflowSpecService failed to return Spec for system type %s and system object %s.\n" +
                            "Error was: %s", systemType, systemObject, e.toString()));
            return null;
        }
        return specs;
    }

}
