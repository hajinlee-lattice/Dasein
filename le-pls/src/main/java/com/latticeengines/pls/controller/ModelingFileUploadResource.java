package com.latticeengines.pls.controller;

import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionRequestParameters;
import static com.latticeengines.pls.util.ImportWorkflowUtils.validateFieldDefinitionsRequestBody;

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
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
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
            boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
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
        return modelingFileMetadataService.validateFieldMappings(csvFileName, fieldMappingDocument, entity, source,
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
            boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
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
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        SourceFile sourceFile = uploadFile("file_" + DateTime.now().getMillis() + ".csv", compressed, csvFileName,
                schemaInterpretation, "", file, false, outsizeFlag);
        try {
            SourceFile resultSourceFile = fileUploadService.uploadCleanupFileTemplate(sourceFile, schemaInterpretation,
                    cleanupOperationType, batonService.isEntityMatchEnabled(customerSpace));
            return ResponseDocument.successResponse(resultSourceFile);
        }  catch (LedpException ledp) {
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            ledp.printStackTrace();
            throw new UIActionException(action, ledp.getCode());
        } catch (RuntimeException e) {
            LedpException ledp = new LedpException(LedpCode.LEDP_18053, new String[] { e.getMessage() });
            UIAction action = graphDependencyToUIActionUtil.generateUIAction(UPLOAD_FILE_ERROR_TITLE, View.Banner,
                    Status.Error, ledp.getMessage());
            throw new UIActionException(action, ledp.getCode());
        }
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

    // Mock API for Import Workflow 2.0 Fetch Field Definitions.
    @RequestMapping(value = "fielddefinition/mockfetch", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<FieldDefinitionsRecord> mockFetchFieldDefinitions(
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject, //
            @RequestParam(value = "importFile", required = true) String importFile) {
        //log.error("JAW ------ BEGIN Mock Fetch Field Definition -----");

        try {
            validateFieldDefinitionRequestParameters("Fetch", systemName, systemType, systemObject, importFile);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }

        // For mock, decide on returned fetchResponse based on request's Template State's system name.
        String fetchResponseFile = null;
        if (systemObject.toLowerCase().contains("account")) {
            fetchResponseFile =
                    "com/latticeengines/pls/controller/internal/account-fetch-field-definition-response.json";
        } else if (systemObject.toLowerCase().contains("contact") || systemObject.toLowerCase().contains("lead")) {
            fetchResponseFile =
                    "com/latticeengines/pls/controller/internal/contact-fetch-field-definition-response.json";
        }

        String fetchResponseJson = "{ \"Result\": \"ERROR: Response processing failure\" }";
        FieldDefinitionsRecord fetchResponse = new FieldDefinitionsRecord();

        try {
            InputStream fetchResponseInputStream = getClass().getClassLoader().getResourceAsStream(fetchResponseFile);
            if (fetchResponseInputStream != null) {
                fetchResponseJson = IOUtils.toString(fetchResponseInputStream, "UTF-8");
                log.error("FetchFieldDefinitionResponse is:\n" + fetchResponseJson);
            } else {
                log.error("Loading Fetch Response failed.");
                return ResponseDocument.failedResponse(new IOException(
                        "Mock Fetch API failed to load mock response from Resource File."));
            }
        } catch (IOException e) {
            log.error("Fetch Response load method threw IOException error:", e);
            return ResponseDocument.failedResponse(e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method threw Exception " + e2.toString(), e2);
            return ResponseDocument.failedResponse(e2);
        }

        if (fetchResponseJson != null) {
            try {
                fetchResponse = JsonUtils.deserialize(fetchResponseJson, FieldDefinitionsRecord.class);
            } catch (Exception e) {
                log.error("JSON deserialization step failed with error:", e);
                ResponseDocument.failedResponse(e);
            }
        } else {
            log.error("===> fetchResponseJson was null!!!");
        }

        //log.error("JAW ------ END Mock Fetch Field Definition -----");

        return ResponseDocument.successResponse(fetchResponse);
    }

    @RequestMapping(value = "fielddefinition/mockvalidate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<ValidateFieldDefinitionsResponse> mockValidateFieldDefinitions(
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject, //
            @RequestParam(value = "importFile", required = true) String importFile, //
            @RequestBody(required = true) ValidateFieldDefinitionsRequest validateRequest) {
        //log.error("JAW ------ BEGIN Mock Validate Field Definition -----");
        //log.error("validateRequest is:\n" + validateRequest.toString());

        try {
            validateFieldDefinitionRequestParameters("Validate", systemName, systemType, systemObject, importFile);
            validateFieldDefinitionsRequestBody("Validate", validateRequest);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }

        ValidateFieldDefinitionsResponse validateResponse = new ValidateFieldDefinitionsResponse();

        // Decide how to handle the Validation Request for mock.  For now, provide either PASS, WARNING, or ERROR
        // response depending on Template State page number.
        int modulo = systemName.length() % 3;
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
            validateResponse.setFieldDefinitionsChangesMap(validateRequest.getFieldDefinitionsChangesMap());

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
            validateResponse.setFieldDefinitionsChangesMap(validateRequest.getFieldDefinitionsChangesMap());
        }

        // For now, set fieldDefinitionsRecordsMap and fieldDefinitionsChangesMap to the values provided at input.
        validateResponse.setFieldDefinitionsRecordsMap(validateRequest.getFieldDefinitionsRecordsMap());

        //log.error("JAW ------ END Mock Validate Field Definition -----");

        return ResponseDocument.successResponse(validateResponse);
    }

    @RequestMapping(value = "fielddefinition/mockcommit", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<FieldDefinitionsRecord> mockCommitFieldDefinitions(
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject, //
            @RequestParam(value = "importFile", required = true) String importFile, //
            @RequestBody(required = true) FieldDefinitionsRecord commitRequest) {
        //log.error("JAW ------ BEGIN Mock Commit Field Definition -----");
        //log.error("commitRequest is: " + commitRequest.toString());

        try {
            validateFieldDefinitionRequestParameters("Commit", systemName, systemType, systemObject, importFile);
            // Make sure that the commit request has field definition records section.
            validateFieldDefinitionsRequestBody("Commit", commitRequest);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }

        FieldDefinitionsRecord commitResponse = new FieldDefinitionsRecord();
        commitResponse.setFieldDefinitionsRecordsMap(commitRequest.getFieldDefinitionsRecordsMap());

        //log.error("JAW ------ END Mock Commit Field Definition -----");
        return ResponseDocument.successResponse(commitResponse);
    }

    // Real API for Import Workflow 2.0 Fetch Field Definitions.
    @RequestMapping(value = "fielddefinition/fetch", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<FieldDefinitionsRecord> fetchFieldDefinitions(
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject, //
            @RequestParam(value = "importFile", required = true) String importFile) {
        try {
            FieldDefinitionsRecord fetchResponse = modelingFileMetadataService.fetchFieldDefinitions(
                    systemName, systemType, systemObject, importFile);
            // TODO(jwinter): Determine if we need to handle a null fetchResponse.
            return ResponseDocument.successResponse(fetchResponse);
        } catch (Exception e) {
            log.error("Fetch Field Definition Failed with Exception: ", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    // Real API for Import Workflow 2.0 Commit Field Definitions.
    @RequestMapping(value = "fielddefinition/commit", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public ResponseDocument<FieldDefinitionsRecord> CommitFieldDefinitions(
            @RequestParam(value = "systemName", required = true) String systemName, //
            @RequestParam(value = "systemType", required = true) String systemType, //
            @RequestParam(value = "systemObject", required = true) String systemObject, //
            @RequestParam(value = "importFile", required = true) String importFile, //
            @RequestBody(required = true) FieldDefinitionsRecord commitRequest) {
        ///log.error("JAW ------ BEGIN Real Commit Field Definition -----");
        //log.error("commitRequest is: " + commitRequest.toString());

        try {
            FieldDefinitionsRecord commitResponse = modelingFileMetadataService.commitFieldDefinitions(
                    systemName, systemType, systemObject, importFile, commitRequest);
            // TODO(jwinter): Determine if we need to handle a null fetchResponse.
            return ResponseDocument.successResponse(commitResponse);
        } catch (Exception e) {
            log.error("Real Commit Failed with Exception: ", e);
            return ResponseDocument.failedResponse(e);
        }
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

}
