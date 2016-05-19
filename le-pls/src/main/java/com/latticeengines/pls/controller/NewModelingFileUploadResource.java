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

    private static FieldMappingDocument DOCUMENT = new FieldMappingDocument();
    private static FieldMapping FIELD_1 = new FieldMapping();
    private static FieldMapping FIELD_2 = new FieldMapping();
    private static FieldMapping FIELD_3 = new FieldMapping();
    private static FieldMapping FIELD_4 = new FieldMapping();
    private static FieldMapping FIELD_5 = new FieldMapping();
    private static FieldMapping FIELD_6 = new FieldMapping();
    private static FieldMapping FIELD_7 = new FieldMapping();
    private static FieldMapping FIELD_8 = new FieldMapping();

    @RequestMapping(value="{sourceFileName}/fieldmappings", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Decides if the csv is a lead or model based. Returned the best mapping and unknown columns as well as lattice fields")
    public FieldMappingDocument getFieldMappings(@PathVariable String sourceFileName) {
        DOCUMENT.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);

        FIELD_1.setUserField("Email");
        FIELD_1.setMappedField("Email");
        FIELD_1.setMappedToLatticeField(true);
        FIELD_1.setFieldType(Schema.Type.STRING.toString());

        FIELD_2.setUserField("Event");
        FIELD_2.setMappedField("Event");
        FIELD_2.setMappedToLatticeField(true);
        FIELD_2.setFieldType(Schema.Type.STRING.toString());

        FIELD_3.setUserField("Id");
        FIELD_3.setMappedField("Id");
        FIELD_3.setMappedToLatticeField(true);
        FIELD_3.setFieldType(Schema.Type.STRING.toString());

        FIELD_4.setUserField("CustomerWebsiteURL");
        FIELD_4.setMappedToLatticeField(false);

        FIELD_5.setUserField("Industry");
        FIELD_5.setMappedField("Industry");
        FIELD_5.setMappedToLatticeField(true);
        FIELD_5.setFieldType(Schema.Type.STRING.toString());

        FIELD_6.setUserField("FirstName");
        FIELD_6.setMappedToLatticeField(false);

        FIELD_7.setUserField("LastName");
        FIELD_7.setMappedToLatticeField(false);

        FIELD_8.setUserField("Date");
        FIELD_8.setMappedToLatticeField(false);

        DOCUMENT.setFieldMappings(Arrays.asList(FIELD_1, FIELD_2, FIELD_3, FIELD_4, FIELD_5, FIELD_6, FIELD_7, FIELD_8));
        DOCUMENT.setIgnoredFields(new ArrayList<String>());
        return DOCUMENT;
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
    public Map<SchemaInterpretation, List<LatticeSchemaField>> getLatticeSchemaFieldMap() {
        return modelingFileMetadataService.getSchemaToLatticeSchemaFields();
    }
}
