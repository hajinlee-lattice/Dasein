package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.wordnik.swagger.annotations.ApiOperation;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;

import io.swagger.annotations.Api;

@Api(value = "models/uploadfile", description = "REST resource for uploading csv files for modeling")
@RestController
@RequestMapping("/models/uploadfile")
@PreAuthorize("hasRole('View_PLS_Data')")
public class NewModelingFileUploadResource {

    private static final Logger log = Logger.getLogger(NewModelingFileUploadResource.class);

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
                                                    @RequestParam("file") MultipartFile file) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            log.info(String.format("Uploading file %s (csvFileName=%s, compressed=%s)", fileName,
                    csvFileName, compressed));
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();

            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            stream = modelingFileMetadataService.validateHeaderFields(stream, closeableResourcePool, csvFileName);

            return ResponseDocument
                    .successResponse(fileUploadService.uploadFile(fileName, csvFileName, stream));
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

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile( //
                                                    @RequestParam(value = "compressed", required = false) boolean compressed, //
                                                    @RequestParam(value = "displayName", required = true) String csvFileName, //
                                                    @RequestParam("file") MultipartFile file) {
        return uploadFile("file_" + DateTime.now().getMillis() + ".csv", compressed, csvFileName, file);
    }

    @RequestMapping(value="{sourceFileName}/fieldmappings", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Decides if the csv is a lead or model based. Returned the best mapping and unknown columns as well as lattice fields")
    public ResponseDocument<FieldMappingDocument> getFieldMappings(@PathVariable String sourceFileName) {
        return ResponseDocument.successResponse(modelingFileMetadataService.mapFieldDocumentBestEffort(sourceFileName));
    }

    @RequestMapping(value="fieldmappings", method = RequestMethod.POST)
    @ApiOperation(value = "Take user input and resolve all field mappings")
    public void saveFieldMappingDocument( //
                                          @RequestParam(value = "displayName", required = true) String csvFileName,
                                          @RequestBody FieldMappingDocument fieldMappingDocument) {
        modelingFileMetadataService.resolveMetadata(csvFileName, fieldMappingDocument);
    }

    @RequestMapping(value="latticeschema", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "return a map from account and lead to the lattice attribute fields")
    public ResponseDocument<Map<SchemaInterpretation, List<LatticeSchemaField>>> getLatticeSchemaFieldMap() {
        return ResponseDocument.successResponse(modelingFileMetadataService.getSchemaToLatticeSchemaFields());
    }
}
