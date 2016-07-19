package com.latticeengines.pls.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.GzipUtils;
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
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Api(value = "scores/fileuploads", description = "REST resource for uploading csv files for scoring")
@RestController
@RequestMapping("/scores/fileuploads")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringFileUploadResource {

    private static final Log log = LogFactory.getLog(ScoringFileUploadResource.class);

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private ModelSummaryService modelSummary;

    @Autowired
    private MetadataProxy metadataProxy;

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
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();
            if (compressed) {
                stream = GzipUtils.decompressStream(stream);
            }

            SourceFile sourceFile = null;
            ModelSummary summary = modelSummary.getModelSummaryByModelId(modelId);
            if (summary.getModelType().equals(ModelType.PYTHONMODEL.getModelType())) {

                List<Attribute> requiredColumns = modelMetadataService.getRequiredColumns(modelId);
                stream = scoringFileMetadataService.validateHeaderFields(stream, requiredColumns,
                        closeableResourcePool, displayName);

                sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                        SchemaInterpretation.TestingData, displayName, stream);

                Table metadataTable = scoringFileMetadataService.registerMetadataTable(sourceFile, modelId);
                sourceFile.setTableName(metadataTable.getName());
            } else {
                if (!stream.markSupported()) {
                    stream = new BufferedInputStream(stream);
                }
                stream.mark(ValidateFileHeaderUtils.BIT_PER_BYTE * ValidateFileHeaderUtils.BYTE_NUM);
                Set<String> headers = ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
                try {
                    stream.reset();
                } catch (IOException e) {
                    log.error(e);
                    throw new LedpException(LedpCode.LEDP_00002, e);
                }
                sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                        SchemaInterpretation.TestingData, displayName, stream);
                Table table = new Table();
                table.setPrimaryKey(null);
                table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
                table.setDisplayName(sourceFile.getDisplayName());
                for (String header : headers) {
                    Attribute attr = new Attribute();
                    attr.setName(header.replaceAll("[^A-Za-z0-9_]", "_"));
                    attr.setDisplayName(header);
                    attr.setPhysicalDataType(FieldType.STRING.avroTypes()[0]);
                    attr.setNullable(true);
                    attr.setTags(Tag.INTERNAL);
                    table.addAttribute(attr);
                }
                sourceFile.setTableName(table.getName());
                metadataProxy.createTable(MultiTenantContext.getTenant().getId(), table.getName(), table);
            }
            sourceFileService.update(sourceFile);
            return ResponseDocument.successResponse(sourceFile);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, new String[] { displayName });
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18053, new String[] { displayName });
            }
        }
    }
}
