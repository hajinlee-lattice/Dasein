package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AtlasExport", description = "REST resource for atlas export")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/atlas/export")
public class AtlasExportResource {

    @Inject
    private AtlasExportService atlasExportService;

    @Inject
    private S3ExportFolderService s3ExportFolderService;

    @GetMapping(value = "")
    @ApiOperation(value = "Get Export record by UUID")
    public AtlasExport findAtlasExportById(@PathVariable String customerSpace, @RequestParam String uuid) {
        return atlasExportService.getAtlasExport(customerSpace, uuid);
    }

    @PutMapping(value = "")
    @ApiOperation(value = "Update Export")
    public void updateAtlasExport(@PathVariable String customerSpace, @RequestParam String uuid,
                                  @RequestParam MetadataSegmentExport.Status status) {
        AtlasExport atlasExport = atlasExportService.getAtlasExport(customerSpace, uuid);
        atlasExport.setStatus(status);
        atlasExportService.updateAtlasExport(customerSpace, atlasExport);
    }

    @PostMapping(value = "")
    @ApiOperation(value = "Create Export")
    public AtlasExport createAtlasExport(@PathVariable String customerSpace, @RequestBody AtlasExport atlasExport) {
        return atlasExportService.createAtlasExport(customerSpace, atlasExport);
    }

    @GetMapping(value = "/findAll")
    @ApiOperation(value = "Get all atlas exports")
    public List<AtlasExport> findAllAtlasExport(@PathVariable String customerSpace) {
        return atlasExportService.findAll(customerSpace);
    }

    @PostMapping(value = "/systemfiles")
    @ApiOperation(value = "Add export file to systempath")
    public void addFileToSystemPath(@PathVariable String customerSpace, @RequestParam String uuid,
                                    @RequestParam String fileName) {
        atlasExportService.addFileToSystemPath(customerSpace, uuid, fileName);
    }

    @PostMapping(value = "/dropfolderfiles")
    @ApiOperation(value = "Add export file to dropfolder")
    public void addFileToDropFolder(@PathVariable String customerSpace, @RequestParam String uuid,
                                    @RequestParam String fileName) {
        atlasExportService.addFileToDropFolder(customerSpace, uuid, fileName);
    }

    @GetMapping(value = "/dropfolder/path")
    @ApiOperation(value = "Get export dropfolder path")
    public String getDropFolderExportPath(@PathVariable String customerSpace,
                                          @RequestParam AtlasExportType exportType, @RequestParam String datePrefix,
                                          @RequestParam(defaultValue = "") String optionalId,
                                          @RequestParam(defaultValue = "false") boolean withProtocol) {
        if (withProtocol) {
            return s3ExportFolderService.getS3PathWithProtocol(customerSpace,
                    s3ExportFolderService.getDropFolderExportPath(customerSpace, exportType, datePrefix, optionalId));
        } else {
            return s3ExportFolderService.getDropFolderExportPath(customerSpace, exportType, datePrefix, optionalId);
        }
    }

    @GetMapping(value = "/system/path")
    @ApiOperation(value = "Get export dropfolder path")
    public String getSystemExportPath(@PathVariable String customerSpace,
                                      @RequestParam(defaultValue = "false") boolean withProtocol) {
        if (withProtocol) {
            return s3ExportFolderService.getS3PathWithProtocol(customerSpace,
                    s3ExportFolderService.getSystemExportPath(customerSpace));
        } else {
            return s3ExportFolderService.getSystemExportPath(customerSpace);
        }
    }

    @PostMapping(value = "/s3/path")
    @ApiOperation(value = "Get s3 path")
    public String getS3PathWithProtocol(@PathVariable String customerSpace, @RequestBody String relativePath) {
        return s3ExportFolderService.getS3PathWithProtocol(customerSpace, relativePath);
    }
}
