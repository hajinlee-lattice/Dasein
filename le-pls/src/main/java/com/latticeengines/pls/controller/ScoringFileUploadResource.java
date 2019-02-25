package com.latticeengines.pls.controller;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scores/fileuploads", description = "REST resource for uploading csv files for scoring")
@RestController
@RequestMapping("/scores/fileuploads")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringFileUploadResource {

    private static final Logger log = LoggerFactory.getLogger(ScoringFileUploadResource.class);

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${pls.fileupload.maxupload.bytes}")
    private long maxUploadSize;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam(value = "displayName", required = true) String displayName, //
            @RequestParam(value = "modelId") String modelId, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam("file") MultipartFile file) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092,
                        new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();
            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            SourceFile sourceFile = null;
            ModelSummary summary = modelSummaryProxy.getByModelId(modelId);
            if (summary.getModelType().equals(ModelType.PYTHONMODEL.getModelType())) {
                stream = scoringFileMetadataService.validateHeaderFields(stream,
                        closeableResourcePool, displayName);

                sourceFile = fileUploadService.uploadFile(
                        "file_" + DateTime.now().getMillis() + ".csv",
                        SchemaInterpretation.TestingData, null, displayName, stream);
            } else {
                if (!stream.markSupported()) {
                    stream = new BufferedInputStream(stream);
                }
                stream.mark(
                        ValidateFileHeaderUtils.BIT_PER_BYTE * ValidateFileHeaderUtils.BYTE_NUM);
                Set<String> headers = ValidateFileHeaderUtils.getCSVHeaderFields(stream,
                        closeableResourcePool);
                try {
                    stream.reset();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new LedpException(LedpCode.LEDP_00002, e);
                }
                scoringFileMetadataService.validateHeadersWithDataCloudAttr(headers);
                sourceFile = fileUploadService.uploadFile(
                        "file_" + DateTime.now().getMillis() + ".csv",
                        SchemaInterpretation.TestingData, null, displayName, stream);
                Table table = new Table();
                table.setPrimaryKey(null);
                table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
                table.setDisplayName(sourceFile.getDisplayName());
                for (String header : headers) {
                    Attribute attr = new Attribute();
                    attr.setName(
                            ValidateFileHeaderUtils.convertFieldNameToAvroFriendlyFormat(header));
                    attr.setDisplayName(header);
                    attr.setPhysicalDataType(FieldType.STRING.avroTypes()[0]);
                    attr.setNullable(true);
                    attr.setTags(Tag.INTERNAL);
                    table.addAttribute(attr);
                }
                table.deduplicateAttributeNames();
                sourceFile.setTableName(table.getName());
                metadataProxy.createTable(MultiTenantContext.getTenant().getId(), table.getName(),
                        table);

                sourceFileService.update(sourceFile);
            }
            return ResponseDocument.successResponse(sourceFile);
        } catch (IOException e) {
            return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18053, new String[] { displayName }));
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                return ResponseDocument.failedResponse(new LedpException(LedpCode.LEDP_18053, new String[] { displayName }));
            }
        }
    }

    @RequestMapping(value = "/fieldmappings", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Takes a file and get first round of field mappings through best effort")
    public ResponseDocument<FieldMappingDocument> getFieldMappings( //
            @RequestParam(value = "csvFileName", required = true) String csvFileName, //
            @RequestParam(value = "modelId", required = true) String modelId) {
        return ResponseDocument.successResponse(
                scoringFileMetadataService.mapRequiredFieldsWithFileHeaders(csvFileName, modelId));
    }

    @RequestMapping(value = "fieldmappings/resolve", method = RequestMethod.POST)
    @ApiOperation(value = "Take user input and resolve required field mappings for scoring")
    public void saveFieldMappingDocument( //
            @RequestParam(value = "csvFileName", required = true) String csvFileName,
            @RequestParam(value = "modelId", required = true) String modelId,
            @RequestBody FieldMappingDocument fieldMappingDocument) {
        scoringFileMetadataService.saveFieldMappingDocument(csvFileName, modelId,
                fieldMappingDocument);
    }

    @RequestMapping(value = "/headerfields", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Returns the header fields for a uploaded file")
    public ResponseDocument<Set<String>> getHeaderFields(
            @RequestParam(value = "csvFileName", required = true) String csvFileName) {
        return ResponseDocument
                .successResponse(scoringFileMetadataService.getHeaderFields(csvFileName));
    }
}
