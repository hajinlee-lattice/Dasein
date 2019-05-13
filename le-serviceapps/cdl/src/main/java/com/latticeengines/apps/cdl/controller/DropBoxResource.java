package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.FileProperty;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dropbox", description = "REST resource for atlas drop box")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dropbox")
public class DropBoxResource {

    @Inject
    private DropBoxService dropBoxService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @GetMapping("")
    @ApiOperation(value = "Get drop box summary")
    public DropBoxSummary getDropBox(@PathVariable String customerSpace) {
        return dropBoxService.getDropBoxSummary();
    }

    @PutMapping("/access")
    @ApiOperation(value = "Grant external access to drop box")
    public GrantDropBoxAccessResponse grantAccess(@PathVariable String customerSpace,
                                                  @RequestBody GrantDropBoxAccessRequest request) {
        return dropBoxService.grantAccess(request);
    }

    @DeleteMapping("/access")
    @ApiOperation(value = "Revoke external access to drop box")
    public SimpleBooleanResponse revokeAccess(@PathVariable String customerSpace) {
        dropBoxService.revokeAccess();
        return SimpleBooleanResponse.successResponse();
    }

    @PutMapping("/key")
    @ApiOperation(value = "Refresh AWS access key to drop box, if the access was granted to a Lattice user")
    public GrantDropBoxAccessResponse refreshAccessKey(@PathVariable String customerSpace) {
        return dropBoxService.refreshAccessKey();
    }

    @PostMapping("")
    @ApiOperation(value = "Create drop box. (only for fixing old tenants)")
    public DropBoxSummary createDropBox(@PathVariable String customerSpace) {
        DropBox dropBox = dropBoxService.create();
        DropBoxSummary summary = new DropBoxSummary();
        summary.setBucket(customersBucket);
        summary.setDropBox(dropBox.getDropBox());
        return summary;
    }

    @PostMapping(value = "/folder/{objectName}")
    @ApiOperation(value = "Create template folder")
    public boolean createFolder(@PathVariable String customerSpace, @PathVariable String objectName,
                                @RequestParam(required = false) String path,
                                @RequestParam(required = false) String systemName) {
        dropBoxService.createFolder(customerSpace, systemName, objectName, path);
        return true;
    }

    @GetMapping(value = "/folder/{systemName}")
    @ApiOperation(value = "Create template folder")
    public boolean createFolder(@PathVariable String customerSpace, @PathVariable String systemName) {
        dropBoxService.createFolder(customerSpace, systemName, null, null);
        return true;
    }

    @GetMapping(value = "/folder")
    @ApiOperation(value = "Get all sub folders")
    public List<String> getAllSubFolders(@PathVariable String customerSpace,
                                         @RequestParam(required = false) String systemName,
                                         @RequestParam(required = false) String objectName,
                                         @RequestParam(required = false) String path) {
        return dropBoxService.getDropFolders(customerSpace, systemName, objectName, path);
    }

    @RequestMapping(value = "/import", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation("Import file to s3")
    public boolean importFileToS3(@PathVariable String customerSpace,
                                  @RequestParam("s3Path") String s3Path,
                                  @RequestParam("hdfsPath") String hdfsPath,
                                  @RequestParam("filename") String filename) {
        return dropBoxService.uploadFileToS3(customerSpace, s3Path, filename, hdfsPath);
    }

    @GetMapping(value = "/fileList")
    @ApiOperation(value = "Get all files under path")
    public List<FileProperty> getAllSubFolders(@PathVariable String customerSpace,
                                               @RequestParam String s3Path,
                                               @RequestParam(required = false, defaultValue = "") String filter) {
        return dropBoxService.getFileListForPath(customerSpace, s3Path, filter);
    }
}
