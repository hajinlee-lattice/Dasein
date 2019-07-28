package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.AvailableDateFormat;
import com.latticeengines.domain.exposed.pls.frontend.CommitFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.CommitFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationMessage;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "models/uploadfile", description = "REST resource for uploading csv files for modeling")
@RestController
@RequestMapping("/models/uploadfile")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelingFileUploadResource {

    private static final Logger log = LoggerFactory.getLogger(ModelingFileUploadResource.class);
    public static final String UPLOAD_FILE_ERROR_TITLE = "Error In File Uploading.";

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Autowired
    private BatonService batonService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam("fileName") String fileName, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam(value = "displayName") String csvFileName, //
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation, //
            @RequestParam(value = "entity", required = false, defaultValue = "") String entity,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "outsizeFlag", required = false, defaultValue = "false") boolean outsizeFlag) {
        return ResponseDocument.successResponse(
                uploadFile(fileName, compressed, csvFileName, schemaInterpretation, entity, file, true,outsizeFlag));
    }

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam(value = "displayName") String csvFileName, //
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation, //
            @RequestParam(value = "entity", required = false) String entity, //
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "outsizeFlag", required = false, defaultValue = "false") boolean outsizeFlag) {
        return uploadFile("file_" + DateTime.now().getMillis() + ".csv", compressed, csvFileName, schemaInterpretation,
                entity, file, outsizeFlag);
    }

    @RequestMapping(value = "{sourceFileName}/fieldmappings", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Decides if the csv is a lead or model based. Returned the best mapping and unknown columns as well as lattice fields")
    public ResponseDocument<FieldMappingDocument> getFieldMappings( //
            @PathVariable String sourceFileName,
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation,
            @RequestParam(value = "entity", required = false, defaultValue = "") String entity,
            @RequestParam(value = "source", required = false, defaultValue = "") String source,
            @RequestParam(value = "feedType", required = false, defaultValue = "") String feedType,
            @RequestBody(required = false) ModelingParameters parameters) {
        if (!StringUtils.isEmpty(entity)) {
            schemaInterpretation = SchemaInterpretation.getByName(entity);
        }
        boolean hasCgProduct = batonService.hasProduct(MultiTenantContext.getCustomerSpace(),
                LatticeProduct.CG);
        if (!hasCgProduct || StringUtils.isEmpty(entity) || StringUtils.isEmpty(source)) {
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
            return ResponseDocument.successResponse(modelingFileMetadataService
                    .getFieldMappingDocumentBestEffort(sourceFileName, schemaInterpretation, parameters, true, false, enableEntityMatch));
        } else {
            return ResponseDocument.successResponse(modelingFileMetadataService
                    .getFieldMappingDocumentBestEffort(sourceFileName, entity, source, feedType));
        }
    }

    @RequestMapping(value = "/validate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Validate csv field mapping.")
    public List<FieldValidation> validateFieldMappingDocumnet( //
           @RequestParam(value = "displayName") String csvFileName,
           @RequestParam(value = "entity") String entity,
           @RequestParam(value = "source", defaultValue = "File") String source,
           @RequestParam(value = "feedType") String feedType,
           @RequestBody FieldMappingDocument fieldMappingDocument) {
        return modelingFileMetadataService
                .validateFieldMappings(csvFileName, fieldMappingDocument, entity, source,
                        feedType);
    }

    @RequestMapping(value = "fieldmappings", method = RequestMethod.POST)
    @ApiOperation(value = "Take user input and resolve all field mappings")
    public void saveFieldMappingDocument( //
            @RequestParam(value = "displayName") String csvFileName,
            @RequestParam(value = "entity", required = false, defaultValue = "") String entity,
            @RequestParam(value = "source", required = false, defaultValue = "") String source,
            @RequestParam(value = "feedType", required = false, defaultValue = "") String feedType,
            @RequestBody FieldMappingDocument fieldMappingDocument) {
        boolean hasCgProduct = batonService.hasProduct(MultiTenantContext.getCustomerSpace(),
                LatticeProduct.CG);
        if (fieldMappingDocument != null) {
            fieldMappingDocument.dedupFieldMappings();
        }
        if (!hasCgProduct || StringUtils.isEmpty(entity) || StringUtils.isEmpty(source)) {
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
            modelingFileMetadataService.resolveMetadata(csvFileName, fieldMappingDocument, true, enableEntityMatch);
        } else {
            modelingFileMetadataService.resolveMetadata(csvFileName, fieldMappingDocument, entity, source, feedType);
        }
    }

    @RequestMapping(value = "latticeschema", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return a map from account and lead to the lattice attribute fields")
    public ResponseDocument<Map<SchemaInterpretation, List<LatticeSchemaField>>> getLatticeSchemaFieldMap(
            @RequestParam(value = "excludeLatticeDataAttributes", required = false, defaultValue = "false") boolean excludeLatticeDataAttributes,
            @RequestParam(value = "entity", required = false, defaultValue = "") String entity,
            @RequestParam(value = "source", required = false, defaultValue = "") String source,
            @RequestParam(value = "feedType", required = false, defaultValue = "") String feedType) {
        boolean hasCgProduct = batonService.hasProduct(MultiTenantContext.getCustomerSpace(),
                LatticeProduct.CG);
        if (!hasCgProduct || StringUtils.isEmpty(entity)) {
            return ResponseDocument.successResponse(
                    modelingFileMetadataService.getSchemaToLatticeSchemaFields(excludeLatticeDataAttributes));
        } else {
            SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
            return ResponseDocument.successResponse(ImmutableMap.of(schemaInterpretation,
                    modelingFileMetadataService.getSchemaToLatticeSchemaFields(entity, source, feedType)));
        }
    }

    @RequestMapping(value = "/dateformat", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return supported date/time format and timezone.")
    public ResponseDocument<AvailableDateFormat> getSupportedDateTimeFormat() {
        AvailableDateFormat availableDateFormat = new AvailableDateFormat();
        availableDateFormat.setDateFormats(TimeStampConvertUtils.getSupportedUserDateFormats());
        availableDateFormat.setTimeFormats(TimeStampConvertUtils.getSupportedUserTimeFormats());
        availableDateFormat.setTimezones(TimeStampConvertUtils.getSupportedUserTimeZones());
        return ResponseDocument.successResponse(availableDateFormat);
    }

    @RequestMapping(value = "/uploaddeletefiletemplate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadDeleteFileTemplate( //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam(value = "displayName") String csvFileName, //
            @RequestParam(value = "schema") SchemaInterpretation schemaInterpretation, //
            @RequestParam(value = "operationType") CleanupOperationType cleanupOperationType, //
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "outsizeFlag", required = false, defaultValue = "false") boolean outsizeFlag) {
        if (schemaInterpretation != SchemaInterpretation.DeleteAccountTemplate
                && schemaInterpretation != SchemaInterpretation.DeleteContactTemplate
                && schemaInterpretation != SchemaInterpretation.DeleteTransactionTemplate) {
            throw new LedpException(LedpCode.LEDP_18173, new String[] { schemaInterpretation.name() });
        }

        SourceFile sourceFile = uploadFile("file_" + DateTime.now().getMillis() + ".csv", compressed, csvFileName,
                schemaInterpretation, "", file, false, outsizeFlag);

        return ResponseDocument.successResponse(
                fileUploadService.uploadCleanupFileTemplate(sourceFile, schemaInterpretation, cleanupOperationType));
    }

    @RequestMapping(value = "/cdlexternalsystems", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return a map with all existed external systems.")
    public ResponseDocument<Map<String, List<String>>> getCDLExternalSystemMap(
            @RequestParam(value = "entity", required = false, defaultValue = "Account") String entity) {
        CDLExternalSystem externalSystem = cdlExternalSystemProxy
                .getCDLExternalSystem(MultiTenantContext.getCustomerSpace().toString(), entity);
        Map<String, List<String>> result = new HashMap<>();
        if (externalSystem == null) {
            for (CDLExternalSystemType type : CDLExternalSystemType.values()) {
                result.put(type.name(), new ArrayList<>());
            }
        } else {
            result.put(CDLExternalSystemType.CRM.name(), externalSystem.getCRMIdList());
            result.put(CDLExternalSystemType.MAP.name(), externalSystem.getMAPIdList());
            result.put(CDLExternalSystemType.ERP.name(), externalSystem.getERPIdList());
            result.put(CDLExternalSystemType.OTHER.name(), externalSystem.getOtherIdList());
        }
        return ResponseDocument.successResponse(result);
    }

    @RequestMapping(value = "/importFile", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Import a file from s3")
    public ResponseDocument<SourceFile> importFile( //
                                                    @RequestBody FileProperty csvFile, //
                                                    @RequestParam(value = "entity") String entity) {
        return ResponseDocument.successResponse(
                uploadFileFromS3(csvFile, entity));
    }

    @RequestMapping(value = "fielddefinition/fetch", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<FetchFieldDefinitionsResponse> fetchFieldDefinitions(
            @RequestParam(value = "tenantId", required =  true) String tenantId, //
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "importFile", required = true) String importFile) {
            //@RequestBody(required = true) FetchFieldDefinitionsRequest fetchRequest) {
        log.error("JAW ------ BEGIN Fetch Field Definition -----");

        //log.error("fetchRequest is:\n" + fetchRequest.toString());

        /* Decide what validation is required with params

        // Field Definition Requests must have a Template State section describing which tenant and template is being
        // imported.
        if (fetchRequest.getTemplateState() == null) {
            log.error("Fetch Field Definition missing template state");
            // throw new LedpException(LedpCode.LEDP_18228, new String[] { fetchRequest.toString() });
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18228,
                    new String[] { fetchRequest.toString() }));
        }

        // TODO(jwinter): Add code to validate the Request further.
        // Need to check that Tenant ID is valid.
        // What other parameter checks should be included?

        if (StringUtils.isBlank(systemName) {
            log.error("Validate Field Definition is missing SystemName");
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18229,
                    new String[] { fetchRequest.toString() }));
        }
        */

        try {
            validateFieldDefinitionRequestParameters(tenantId, systemName, systemType, importFile);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }

        // For mock, decide on returned fetchResponse based on request's Template State's system name.
        String fetchResponseFile = null;
        if (systemName.toLowerCase().contains("account")) {
            fetchResponseFile =
                    "com/latticeengines/pls/controller/internal/account-fetch-field-definition-response.json";
        } else if (systemName.toLowerCase().contains("contact")) {
            fetchResponseFile =
                    "com/latticeengines/pls/controller/internal/contact-fetch-field-definition-response.json";
        }

        String fetchResponseJson = "{ \"Result\": \"ERROR: Response processing failure\" }";
        FetchFieldDefinitionsResponse fetchResponse = new FetchFieldDefinitionsResponse();


        try {
            InputStream fetchResponseInputStream = getClass().getClassLoader().getResourceAsStream(fetchResponseFile);
            if (fetchResponseInputStream != null) {
                fetchResponseJson = IOUtils.toString(fetchResponseInputStream, "UTF-8");
                log.error("FetchFieldDefinitionResponse (5) is:\n" + fetchResponseJson);
            } else {
                log.error("Loading Fetch Response failed.");
                return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18230,
                        new String[] { fetchResponseFile }));
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (5) threw IOException error:", e);
            return ResponseDocument.failedResponse(e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (5) threw Exception " + e2.toString(), e2);
            return ResponseDocument.failedResponse(e2);
        }



        /*
        try {
            InputStream fetchResponseInputStream = ClassLoader.getSystemResourceAsStream(
                    "com/latticeengines/pls/controller/internal/fetch-field-definition-response.json");
            if (fetchResponseInputStream != null) {
                fetchResponseJson = IOUtils.toString(fetchResponseInputStream, "UTF-8");
                log.error("FetchFieldDefinitionResponse (1) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (1) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (1) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (1) threw Exception " + e2.toString(), e2);
        }

        try {
            File fetchResponseFile = new File(ClassLoader
                    .getSystemResource(
                            "com/latticeengines/pls/controller/internal/fetch-field-definition-response.json")
                    .getPath());
            if (fetchResponseFile != null) {
                fetchResponseJson = FileUtils.getContentsAsString(fetchResponseFile);
                log.error("FetchFieldDefinitionResponse (2) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (2) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (2) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (2) threw Exception " + e2.toString(), e2);
        }

        try {
            URL fetchResponseUrl = ClassLoader.getSystemResource(
                    "com/latticeengines/pls/controller/internal/fetch-field-definition-response.json");
            if (fetchResponseUrl != null) {
                InputStream fetchResponseInputStream = new FileInputStream(new File(fetchResponseUrl.getPath()));
                fetchResponse = JsonUtils.deserialize(fetchResponseInputStream, FetchFieldDefinitionsResponse.class);

                log.error("FetchFieldDefinitionResponse (3) is:\n" + fetchResponse.toString());
            } else {
                log.error("Fetch Response load method (3) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (3) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (3) threw Exception " + e2.toString(), e2);
        }

        try {
            File fetchResponseFile = new File(ClassLoader
                    .getSystemResource("com/latticeengines/pls/controller/internal/fetch-field-definition-response.json")
                    .getFile());
            if (fetchResponseFile != null) {
                fetchResponseJson = org.apache.commons.io.FileUtils.readFileToString(fetchResponseFile,
                        Charset.defaultCharset());
                log.error("FetchFieldDefinitionResponse (4) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (4) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (4) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (4) threw Exception " + e2.toString(), e2);
        }
        */

        if (fetchResponseJson != null) {
            try {
                fetchResponse = JsonUtils.deserialize(fetchResponseJson, FetchFieldDefinitionsResponse.class);
            } catch (Exception e) {
                log.error("JSON deserialization step failed with error:", e);
                ResponseDocument.failedResponse(e);
            }
        } else {
            log.error("===> fetchResponseJson was null!!!");
        }

        log.error("JAW ------ END Fetch Field Definition -----");

        return ResponseDocument.successResponse(fetchResponse);
    }

    @RequestMapping(value = "fielddefinition/validate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<ValidateFieldDefinitionsResponse> validateFieldDefinitions(
            @RequestParam(value = "tenantId", required =  true) String tenantId, //
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "importFile", required = true) String importFile, //
            @RequestBody(required = true) ValidateFieldDefinitionsRequest validateRequest) {
        log.error("JAW ------ BEGIN Validate Field Definition -----");

        log.error("validateRequest is:\n" + validateRequest.toString());

        /* Decide what validation is required for params

        // Field Definition Requests must have a Template State section describing which tenant and template is being
        // imported.
        if (validateRequest.getTemplateState() == null) {
            log.error("Validate Field Definition missing template state");
            // throw new LedpException(LedpCode.LEDP_18228, new String[] { validateRequest.toString() });
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18228,
                    new String[] { validateRequest.toString() }));
        }

        // TODO(jwinter): Add code to validate the Request further.
        // Need to check that Tenant ID is valid.
        // What other parameter checks should be included?

        if (StringUtils.isBlank(validateRequest.getTemplateState().getSystemName())) {
            log.error("Validate Field Definition is missing SystemName");
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18229,
                    new String[] { validateRequest.toString() }));
        }

        // TODO(jwinter): Fix the parameter checking code.
        if (StringUtils.isBlank(tenantId)) {
            log.error("Validate Field Definition Request has null or blank Tenant ID");
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18228,
                    new String[] { validateRequest.toString() }));
        }
        */

        try {
            validateFieldDefinitionRequestParameters(tenantId, systemName, systemType, importFile);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }


        ValidateFieldDefinitionsResponse validateResponse = new ValidateFieldDefinitionsResponse();

        // Decide how to handle the Validation Request for mock.  For now, provide either PASS, WARNING, or ERROR
        // response depending on Template State page number.


        // TODO(jwinter): Need to validate all input fields exist!

        int modulo = tenantId.length() % 3;

        if (modulo == 0) {
            validateResponse.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.PASS);
        } else if (modulo == 1) {
            validateResponse.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.WARNING);

            for (Map.Entry<String, List<FieldDefinition>> changeEntry :
                    validateRequest.getFieldDefinitionsChangesMap().entrySet()) {
                List<FieldValidationMessage> warningList = new ArrayList<>();
                for (FieldDefinition definition : changeEntry.getValue()) {
                    FieldValidationMessage message = new FieldValidationMessage();
                    message.setFieldName(definition.getFieldName());
                    message.setColumnName(definition.getColumnName());
                    message.setMessageLevel(FieldValidationMessage.MessageLevel.WARNING);
                    message.setMessage(definition.getColumnName() + " has BLAH BLAH minor issue when mapped to " +
                            definition.getFieldName());
                    warningList.add(message);
                }
                validateResponse.addFieldValidationMessages(changeEntry.getKey(), warningList, true);
            }

        } else {
            validateResponse.setValidationResult(ValidateFieldDefinitionsResponse.ValidationResult.ERROR);

            int count = 0;
            for (Map.Entry<String, List<FieldDefinition>> changeEntry :
                    validateRequest.getFieldDefinitionsChangesMap().entrySet()) {
                List<FieldValidationMessage> warningErrorList = new ArrayList<>();
                for (FieldDefinition definition : changeEntry.getValue()) {
                    FieldValidationMessage message = new FieldValidationMessage();
                    message.setFieldName(definition.getFieldName());
                    message.setColumnName(definition.getColumnName());
                    if (count++ % 2 == 0) {
                        message.setMessageLevel(FieldValidationMessage.MessageLevel.ERROR);
                        message.setMessage(definition.getColumnName() + " has OH BOY major problem when mapped to " +
                                definition.getFieldName());
                    } else {
                        message.setMessageLevel(FieldValidationMessage.MessageLevel.WARNING);
                        message.setMessage(definition.getColumnName() + " has BLAH BLAH minor issue when mapped to " +
                                definition.getFieldName());
                    }
                    warningErrorList.add(message);
                }
                validateResponse.addFieldValidationMessages(changeEntry.getKey(), warningErrorList, true);
            }
        }

        // For now, set fieldDefinitionsRecordsMap and fieldDefinitionsChangesMap to the values provided at input.
        validateResponse.setFieldDefinitionsRecordsMap(validateRequest.getFieldDefinitionsRecordsMap());
        validateResponse.setFieldDefinitionsChangesMap(validateRequest.getFieldDefinitionsChangesMap());

        log.error("JAW ------ END Validate Field Definition -----");

        return ResponseDocument.successResponse(validateResponse);
    }

    @RequestMapping(value = "fielddefinition/commit", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<CommitFieldDefinitionsResponse> vaidateFieldDefinitions(
            @RequestParam(value = "tenantId", required =  true) String tenantId, //
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "importFile", required = true) String importFile, //
            @RequestBody(required = true) CommitFieldDefinitionsRequest commitRequest) {
        log.error("JAW ------ BEGIN Commit Field Definition -----");

        log.error("commitRequest is: " + commitRequest.toString());

        /*
        if (commitRequest.getTemplateState() == null) {
            log.error("Commit Field Definition missing template state");
            throw new LedpException(LedpCode.LEDP_18228, new String[] { commitRequest.toString() });
        }


        // Need to check that Tenant ID is valid.
        // What other parameter checks should be included?

        // TODO(jwinter): Fix the parameter checking code.
        if (StringUtils.isBlank(tenantId)) {
            log.error("Commit Field Definition Request has null or blank Tenant ID");
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18228,
                    new String[] { commitRequest.toString() }));
        }
        */

        try {
            validateFieldDefinitionRequestParameters(tenantId, systemName, systemType, importFile);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }






        CommitFieldDefinitionsResponse commitResponse = new CommitFieldDefinitionsResponse();

        if (commitRequest.getFieldDefinitionsRecordsMap() == null) {
            log.error("Commit Request missing Field Definitions Record Map");
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18231,
                    new String[] { commitResponse.toString() }));
        }

        commitResponse.setFieldDefinitionsRecordsMap(commitRequest.getFieldDefinitionsRecordsMap());


        log.error("JAW ------ END Commit Field Definition -----");

        return ResponseDocument.successResponse(commitResponse);
    }

    private SourceFile uploadFile(String fileName, boolean compressed, String csvFileName,
            SchemaInterpretation schemaInterpretation, String entity, MultipartFile file, boolean checkHeaderFormat, boolean outsizeFlag) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            log.info(String.format("Uploading file %s (csvFileName=%s, compressed=%s)", fileName, csvFileName,
                    compressed));
            if (!outsizeFlag && file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();

            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            stream = modelingFileMetadataService.validateHeaderFields(stream, closeableResourcePool, csvFileName,
                    checkHeaderFormat, entity);
            if (!StringUtils.isEmpty(entity)) {
                schemaInterpretation = SchemaInterpretation.getByName(entity);
            }

            return fileUploadService.uploadFile(fileName, schemaInterpretation, entity, csvFileName, stream, outsizeFlag);
        } catch (IOException e) {
            LedpException ledp = new LedpException(LedpCode.LEDP_18053, new String[] { csvFileName });
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            throw new UIActionException(action, ledp.getCode());
        } catch (LedpException ledp) {
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            throw new UIActionException(action, ledp.getCode());
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                LedpException ledp = new LedpException(LedpCode.LEDP_18053, new String[] { csvFileName });
                UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                        Status.Error, ledp.getMessage());
                throw new UIActionException(action, ledp.getCode());
            }
        }
    }

    private SourceFile uploadFileFromS3(FileProperty csvFile, String entity) {
        try {
            log.info(String.format("Uploading file %s (csvFileName=%s)", csvFile.getFileName(), csvFile.getFileName()));
            return fileUploadService.createSourceFileFromS3(csvFile, entity);
        } catch (LedpException ledp) {
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            throw new UIActionException(action, ledp.getCode());
        }
    }

    private void validateFieldDefinitionRequestParameters(String tenantId, String systemName, String systemType,
                                                          String importFile) throws LedpException {
        log.error("Field Definition Request Parameters:\n   tenantId: " + tenantId + "\n   systemName: " + systemName +
                "\n   systemType: " + systemType + "\n   importFile: " + importFile);

        // TODO(jwinter): Figure out what validation is needed.

        if (StringUtils.isBlank(tenantId)) {
            log.error("tenantId is null or blank");
            throw new LedpException(LedpCode.LEDP_18232, new String[] { "tenantId" });
        }

        if (StringUtils.isBlank(systemName)) {
            log.error("systemName is null or blank");
            throw new LedpException(LedpCode.LEDP_18232, new String[] { "systemName" });
        }

        if (StringUtils.isBlank(systemType)) {
            log.error("systemType is null or blank");
            throw new LedpException(LedpCode.LEDP_18232, new String[] { "systemType" });
        }

        if (StringUtils.isBlank(importFile)) {
            log.error("importFile is null or blank");
            throw new LedpException(LedpCode.LEDP_18232, new String[] { "importFile" });
        }
    }
}
