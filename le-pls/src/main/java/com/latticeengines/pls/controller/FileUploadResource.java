package com.latticeengines.pls.controller;

import java.io.ByteArrayInputStream;
import java.rmi.server.UID;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.metadata.exposed.resolution.ColumnTypeMapping;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.workflow.exposed.service.SourceFileService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "fileuploads", description = "REST resource for uploading data files")
@RestController
@RequestMapping("/fileuploads")
@PreAuthorize("hasRole('Edit_PLS_Data')")
public class FileUploadResource {
    private static final Logger log = Logger.getLogger(FileUploadResource.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private FileUploadService fileUploadService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile(@RequestParam("fileName") String fileName,
            @RequestParam("schema") SchemaInterpretation schema, @RequestParam("file") MultipartFile file) {
        try {
            return ResponseDocument.successResponse(fileUploadService.uploadFile(fileName, schema,
                    new ByteArrayInputStream(file.getBytes())));
        } catch (Exception e) {
            log.error(e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile(@RequestParam("schema") SchemaInterpretation schema,
            @RequestParam("file") MultipartFile file) {
        String filename = new UID().toString().replace("-", "").replace(":", "") + ".csv";
        return uploadFile(filename, schema, file);
    }

    @RequestMapping(value = "{fileName}/metadata/unknown", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Retrieve the list of unknown metadata columns")
    public ResponseDocument<List<ColumnTypeMapping>> getUnknownColumns(@PathVariable String fileName) {
        try {
            return ResponseDocument.successResponse(fileUploadService.getUnknownColumns(fileName));
        } catch (Exception e) {
            log.error(e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "{fileName}/metadata/unknown", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Resolve the metadata given the list of specified columns")
    public SimpleBooleanResponse resolveMetadata(@PathVariable String fileName,
            @RequestBody List<ColumnTypeMapping> unknownColumns) {
        try {
            fileUploadService.resolveMetadata(fileName, unknownColumns);
            return SimpleBooleanResponse.successResponse();
        } catch (Exception e) {
            log.error(e);
            return SimpleBooleanResponse.failedResponse(e);
        }
    }

}
