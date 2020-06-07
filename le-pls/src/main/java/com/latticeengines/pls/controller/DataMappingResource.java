package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.pls.service.DataMappingService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datamapping", description = "REST resource for uploading csv files for mapping")
@RestController
@RequestMapping("/datamapping")
@PreAuthorize("hasRole('View_PLS_Data')")
public class DataMappingResource {

    private static final Logger log = LoggerFactory.getLogger(DataMappingResource.class);

    @Inject
    private DataMappingService dataMappingService;

    // API for Import Workflow 2.0 Fetch Field Definitions.
    // Parameters:
    //   systemName: The user defined name for the system for which a template is being created, eg. Marketo 1.
    //   systemType: The type of system for which a template is being created, eg. Salesforce
    //   systemObject: The entity type of this template (also called EntityType.displayName), eg. Accounts
    //   importFile: The name of the CSV file this template is being generated for.
    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Get Data Mapping for a System Object")
    public ResponseDocument<FetchFieldDefinitionsResponse> fetchFieldDefinitions(
            @RequestParam(value = "systemName") String systemName, //
            @RequestParam(value = "systemType") String systemType, //
            @RequestParam(value = "systemObject") String systemObject, //
            @RequestParam(value = "fileImportId", required = false) String fileImportId) {
        try {
            FetchFieldDefinitionsResponse fetchResponse = dataMappingService.fetchFieldDefinitions(
                    systemName, systemType, systemObject, fileImportId);
            return ResponseDocument.successResponse(fetchResponse);
        } catch (Exception e) {
            log.error("Fetch Field Definition Failed with Exception: ", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    // API for Import Workflow 2.0 Fetch Field Definitions.
    // Parameters:
    //   systemName: The user defined name for the system for which a template is being created, eg. Marketo 1.
    //   systemType: The type of system for which a template is being created, eg. Salesforce
    //   systemObject: The entity type of this template (also called EntityType.displayName), eg. Accounts
    //   importFile: The name of the CSV file this template is being generated for.
    // Body:
    // ValidateFieldDefinitionsRequest representing field definition changes/records
    @PostMapping(value = "/validate")
    @ResponseBody
    @ApiOperation(value = "Validate Data Mapping for a System Object")
    public ResponseDocument<ValidateFieldDefinitionsResponse> validateFieldDefinitions(
            @RequestParam(value = "systemName") String systemName, //
            @RequestParam(value = "systemType") String systemType, //
            @RequestParam(value = "systemObject") String systemObject, //
            @RequestParam(value = "fileImportId", required = false) String fileImportId, //
            @RequestBody(required = true) ValidateFieldDefinitionsRequest validateRequest) {
        ValidateFieldDefinitionsResponse validateFieldDefinitionsResponse = null;
        try {
            validateFieldDefinitionsResponse = dataMappingService.validateFieldDefinitions(systemName,
                    systemType, systemObject, fileImportId, validateRequest);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
        return ResponseDocument.successResponse(validateFieldDefinitionsResponse);
    }

    // API for Import Workflow 2.0 Commit Field Definitions.
    // Parameters:
    //   systemName: The user defined name for the system for which a template is being created, eg. Marketo 1.
    //   systemType: The type of system for which a template is being created, eg. Salesforce
    //   systemObject: The entity type of this template (also called EntityType.displayName), eg. Accounts
    //   importFile: The name of the CSV file this template is being generated for.
    //   runImport: Boolean representing if a import workflow job should be initiated upon committing this template.
    // Body:
    //    The FieldDefinitionsRecord representing the field mappings for this template.
    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Set Data Mapping for a System Object")
    public ResponseDocument<FieldDefinitionsRecord> CommitFieldDefinitions(
            @RequestParam(value = "systemName") String systemName, //
            @RequestParam(value = "systemType") String systemType, //
            @RequestParam(value = "systemObject") String systemObject, //
            @RequestParam(value = "fileImportId", required = false) String fileImportId, //
            @RequestParam(value = "runImport", required = false, defaultValue = "false") boolean runImport, //
            @RequestBody(required = true) FieldDefinitionsRecord commitRequest) {
        try {
            FieldDefinitionsRecord commitResponse = dataMappingService.commitFieldDefinitions(
                    systemName, systemType, systemObject, fileImportId, runImport, commitRequest);
            return ResponseDocument.successResponse(commitResponse);
        } catch (Exception e) {
            log.error("Commit Failed with Exception: ", e);
            return ResponseDocument.failedResponse(e);
        }
    }
}
