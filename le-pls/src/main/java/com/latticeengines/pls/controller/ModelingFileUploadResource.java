package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.EntityExternalType;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "models/uploadfile", description = "REST resource for uploading csv files for modeling")
@RestController
@RequestMapping("/models/uploadfile")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelingFileUploadResource {

    private static final Logger log = LoggerFactory.getLogger(ModelingFileUploadResource.class);

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam("fileName") String fileName, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam(value = "displayName", required = true) String csvFileName, //
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation, //
            @RequestParam(value = "entityExternalType", required = false) EntityExternalType entityExternalType, //
            @RequestParam("file") MultipartFile file) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            log.info(String.format("Uploading file %s (csvFileName=%s, compressed=%s)", fileName,
                    csvFileName, compressed));
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092,
                        new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();

            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            stream = modelingFileMetadataService.validateHeaderFields(stream, closeableResourcePool,
                    csvFileName);

            return ResponseDocument.successResponse(fileUploadService.uploadFile(fileName,
                    schemaInterpretation, entityExternalType, csvFileName, stream));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, new String[] { csvFileName });
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18053, new String[] { csvFileName });
            }
        }
    }

    @RequestMapping(value = "/cdl", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. (Template file or data file)")
    public ResponseDocument<SourceFile> uploadFile(@RequestParam("fileName") String fileName, //
                                                   @RequestParam(value = "compressed", required = false) boolean compressed, //
                                                   @RequestParam(value = "displayName") String csvFileName, //
                                                   @RequestParam(value = "entity") String entity, //
                                                   @RequestParam("file") MultipartFile file) {
        try {
            log.info(String.format("Uploading file %s (csvFileName=%s, compressed=%s)", fileName,
                    csvFileName, compressed));

            InputStream stream = file.getInputStream();

            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }
            EntityExternalType entityExternalType = EntityExternalType.getByName(entity);
            SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);

            return ResponseDocument.successResponse(fileUploadService.uploadFile(fileName,
                    schemaInterpretation, entityExternalType, csvFileName, stream));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, new String[] { csvFileName });
        }
    }

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam(value = "displayName", required = true) String csvFileName, //
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation, //
            @RequestParam(value = "entityExternalType", required = false) EntityExternalType entityExternalType, //
            @RequestParam("file") MultipartFile file) {
        return uploadFile("file_" + DateTime.now().getMillis() + ".csv", compressed, csvFileName,
                schemaInterpretation, entityExternalType, file);
    }

    @RequestMapping(value = "{sourceFileName}/fieldmappings", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Decides if the csv is a lead or model based. Returned the best mapping and unknown columns as well as lattice fields")
    public ResponseDocument<FieldMappingDocument> getFieldMappings( //
            @PathVariable String sourceFileName,
            @RequestParam(value = "schema", required = false) SchemaInterpretation schemaInterpretation,
            @RequestBody ModelingParameters parameters) {
        return ResponseDocument.successResponse(
                modelingFileMetadataService.getFieldMappingDocumentBestEffort(sourceFileName,
                        schemaInterpretation, parameters));
    }

    @RequestMapping(value = "{sourceFileName}/fieldmappings/cdl", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get field mappings for a source file.")
    public ResponseDocument<FieldMappingDocument> getFieldMappings(@PathVariable String sourceFileName,
                                                                   @RequestParam(value = "entity") String entity) {
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
        return ResponseDocument.successResponse(
                modelingFileMetadataService.getFieldMappingDocumentBestEffort(sourceFileName,
                        schemaInterpretation, null));
    }

    @RequestMapping(value = "fieldmappings", method = RequestMethod.POST)
    @ApiOperation(value = "Take user input and resolve all field mappings")
    public void saveFieldMappingDocument( //
            @RequestParam(value = "displayName", required = true) String csvFileName,
            @RequestBody FieldMappingDocument fieldMappingDocument) {
        modelingFileMetadataService.resolveMetadata(csvFileName, fieldMappingDocument);
    }

    @RequestMapping(value = "latticeschema", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return a map from account and lead to the lattice attribute fields")
    public ResponseDocument<Map<SchemaInterpretation, List<LatticeSchemaField>>> getLatticeSchemaFieldMap(
            @RequestParam(value = "excludeLatticeDataAttributes", required = false, defaultValue = "false") boolean excludeLatticeDataAttributes) {
        return ResponseDocument.successResponse(modelingFileMetadataService
                .getSchemaToLatticeSchemaFields(excludeLatticeDataAttributes));
    }

    @RequestMapping(value = "latticeschema/cdl", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return a list of the lattice attribute fields for certain entity type")
    public ResponseDocument<List<LatticeSchemaField>> getLatticeSchemaFieldMap(
            @RequestParam(value = "entity") String entity) {
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.getByName(entity);
        return ResponseDocument.successResponse(modelingFileMetadataService
                .getSchemaToLatticeSchemaFields(schemaInterpretation));
    }
}
