package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
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
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scores/fileuploads", description = "REST resource for uploading csv files for scoring")
@RestController
@RequestMapping("/scores/fileuploads")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringFileUploadResource {

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private ModelMetadataService modelMetadataService;

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

            List<Attribute> requiredColumns = modelMetadataService.getRequiredColumns(modelId);
            stream = scoringFileMetadataService.validateHeaderFields(stream, requiredColumns, closeableResourcePool,
                    displayName);

            SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                    SchemaInterpretation.TestingData, displayName, stream);

            Table metadataTable = scoringFileMetadataService.registerMetadataTable(sourceFile, modelId);
            sourceFile.setTableName(metadataTable.getName());

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

    @RequestMapping(value = "{fileName}/import/errors", method = RequestMethod.GET, produces = "text/plain")
    @ResponseBody
    @ApiOperation(value = "Retrieve file import errors")
    public void getImportErrors(@PathVariable String fileName, HttpServletResponse response) {
        try {
            InputStream is = fileUploadService.getImportErrorStream(fileName);
            response.setContentType("text/plain");
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", "errors.csv"));
            IOUtils.copy(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18093, e);
        }
    }
}
