package com.latticeengines.pls.controller;

import java.io.ByteArrayInputStream;
import java.rmi.server.UID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.domain.exposed.workflow.SourceFileSchema;
import com.latticeengines.pls.service.FileUploadService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "fileuploads", description = "REST resource for uploading data files")
@RestController
@RequestMapping("/fileuploads")
@PreAuthorize("hasRole('Edit_PLS_Data')")
public class FileUploadResource {

    @Autowired
    private FileUploadService fileUploadService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public ResponseDocument<SourceFile> uploadFile(@RequestParam("name") String name,
            @RequestParam("schema") SourceFileSchema schema, @RequestParam("file") MultipartFile file) {
        try {
            return new ResponseDocument<>(fileUploadService.uploadFile(name, schema,
                    new ByteArrayInputStream(file.getBytes())));
        } catch (Exception e) {
            return new ResponseDocument<>(e);
        }
    }

    @RequestMapping(value = "/unnamed", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a file. The server will create a unique name for the file")
    public ResponseDocument<SourceFile> uploadFile(@RequestParam("schema") SourceFileSchema schema,
            @RequestParam("file") MultipartFile file) {
        String filename = new UID().toString().replace("-", "").replace(":", "") + ".tmp";
        return uploadFile(filename, schema, file);
    }

}
