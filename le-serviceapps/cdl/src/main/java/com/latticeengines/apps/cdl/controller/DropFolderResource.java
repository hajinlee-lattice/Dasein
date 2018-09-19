package com.latticeengines.apps.cdl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DropFolderService;

@Api(value = "dropfolder", description = "REST resource for drop folder")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/dropfolder")
public class DropFolderResource {

    private static final Logger log = LoggerFactory.getLogger(DropFolderResource.class);

    @Inject
    private DropFolderService dropFolderService;

    @PostMapping(value = "/{objectName}")
    @ApiOperation(value = "Create template folder")
    public boolean createFolder(@PathVariable String customerSpace, @PathVariable String objectName,
                                @RequestParam(value = "", required = false) String path) {
        dropFolderService.createFolder(customerSpace, objectName, path);
        return true;
    }

    @GetMapping(value = "")
    @ApiOperation(value = "Get all sub folders")
    public List<String> getAllSubFolders(@PathVariable String customerSpace,
                                         @RequestParam(value = "", required = false) String objectName,
                                         @RequestParam(value = "", required = false) String path) {
        return dropFolderService.getDropFolders(customerSpace, objectName, path);
    }
}
