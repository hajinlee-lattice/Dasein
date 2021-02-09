package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "sourcefile", description = "REST resource for sourcefiles")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/sourcefiles")
public class SourceFileResource {

    @Inject
    private SourceFileService sourceFileService;

    // use request param instead of path variable
    // because if file name has extension (.csv)
    // spring will confused with its json representation
    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Find source file by name")
    public SourceFile findByName(@PathVariable String customerSpace, @RequestParam(name = "name") String name) {
        return sourceFileService.findByName(name);
    }

    @GetMapping("/tablename/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Find source file by table name")
    public SourceFile findByTableName(@PathVariable String customerSpace, @PathVariable String tableName) {
        return sourceFileService.findByTableName(tableName);
    }

    @GetMapping("/applicationid/{applicationId}")
    @ResponseBody
    @ApiOperation(value = "Find source file by application Id")
    public SourceFile findByApplicationId(@PathVariable String customerSpace, @PathVariable String applicationId) {
        return sourceFileService.findByApplicationId(applicationId);
    }

    @GetMapping("/workflowpid/{workflowPid}")
    @ResponseBody
    @ApiOperation(value = "Find source file by workflow pid")
    public SourceFile findByWorkflowPid(@PathVariable String customerSpace, @PathVariable String workflowPid) {
        return sourceFileService.findByWorkflowPid(Long.parseLong(workflowPid));
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create source file")
    public void create(@PathVariable String customerSpace, @RequestBody SourceFile sourceFile) {
        sourceFileService.create(sourceFile);
    }

    @PutMapping("")
    @ResponseBody
    @ApiOperation(value = "Update source file")
    public void update(@PathVariable String customerSpace, @RequestBody SourceFile sourceFile) {
        sourceFileService.update(sourceFile);
    }

    @DeleteMapping("/name/{name:.+}")
    @ResponseBody
    @ApiOperation(value = "Delete source file by name")
    public void delete(@PathVariable String customerSpace, @PathVariable String name) {
        sourceFileService.delete(name);
    }

    @PostMapping("/copy")
    @ResponseBody
    @ApiOperation(value = "Copy source file to the target tenant")
    public void copySourceFile(@PathVariable String customerSpace, @RequestBody CopySourceFileRequest request) {
        sourceFileService.copySourceFile(request);
    }

    @PostMapping("/fromS3")
    @ApiOperation(value = "Get file inputStream from s3")
    public ResponseDocument<SourceFile> createSourceFileFromS3(@PathVariable String customerSpace,
                                                               @RequestParam(value = "entity") String entity,
                                                               @RequestParam(value = "schema") String schema,
                                                               @RequestBody FileProperty fileProperty) {
        try {
            SourceFile sourceFile = sourceFileService.createSourceFileFromS3(customerSpace, fileProperty, entity, schema);
            return ResponseDocument.successResponse(sourceFile);
        }catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }
}
