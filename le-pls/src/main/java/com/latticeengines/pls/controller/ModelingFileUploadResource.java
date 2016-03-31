package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.ZipUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "models/fileuploads", description = "REST resource for uploading csv files for modeling")
@RestController
@RequestMapping("/models/fileuploads")
public class ModelingFileUploadResource {
    // TODO rights

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ModelingFileUploadResource.class);

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.fileupload.maxUpload.bytes}")
    private long maxUploadSize;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam("fileName") String fileName, //
            @RequestParam("schema") SchemaInterpretation schema, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam("file") MultipartFile file) {
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            if (file.getSize() >= maxUploadSize) {
                throw new LedpException(LedpCode.LEDP_18092, new String[] { Long.toString(maxUploadSize) });
            }

            InputStream stream = file.getInputStream();
            if (compressed) {
                stream = ZipUtils.decompressStream(stream);
            }

            stream = modelingFileMetadataService.validateHeaderFields(stream, schema, closeableResourcePool, fileName);

            return ResponseDocument.successResponse(fileUploadService.uploadFile(fileName, schema, stream));
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, new String[] { fileName });
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18053, new String[] { fileName });
            }
        }
    }

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile( //
            @RequestParam("schema") SchemaInterpretation schema, //
            @RequestParam(value = "compressed", required = false) boolean compressed, //
            @RequestParam("file") MultipartFile file) {
        return uploadFile("file_" + DateTime.now().getMillis() + ".csv", schema, compressed, file);
    }

    @RequestMapping(value = "{fileName}/metadata", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Retrieve the metadata of the specified source file")
    public ResponseDocument<Table> getMetadata(@PathVariable String fileName) {
        return ResponseDocument.successResponse(fileUploadService.getMetadata(fileName));
    }

    @RequestMapping(value = "{fileName}/metadata/unknown", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Retrieve the list of unknown metadata columns")
    public ResponseDocument<List<ColumnTypeMapping>> getUnknownColumns(@PathVariable String fileName) {
        return ResponseDocument.successResponse(modelingFileMetadataService.getUnknownColumns(fileName));
    }

    @RequestMapping(value = "{fileName}/metadata/unknown", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Resolve the metadata given the list of specified columns")
    public SimpleBooleanResponse resolveMetadata(@PathVariable String fileName,
            @RequestBody List<ColumnTypeMapping> unknownColumns) {
        modelingFileMetadataService.resolveMetadata(fileName, unknownColumns);
        return SimpleBooleanResponse.successResponse();
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
