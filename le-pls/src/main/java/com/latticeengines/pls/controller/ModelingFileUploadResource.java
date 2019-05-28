package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

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
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
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
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
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
            return ResponseDocument.successResponse(modelingFileMetadataService
                    .getFieldMappingDocumentBestEffort(sourceFileName, schemaInterpretation, parameters, false, false));
        } else {
            return ResponseDocument.successResponse(modelingFileMetadataService
                    .getFieldMappingDocumentBestEffort(sourceFileName, entity, source, feedType));
        }
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
            modelingFileMetadataService.resolveMetadata(csvFileName, fieldMappingDocument);
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
                    checkHeaderFormat, !StringUtils.isEmpty(entity));
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
