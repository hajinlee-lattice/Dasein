package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadatauploads", description = "REST resource for uploading metadata files")
@RestController
@RequestMapping("/metadatauploads")
@PreAuthorize("hasRole('Edit_PLS_Models')")
public class MetadataFileUploadResource {

    private static final Logger log = Logger.getLogger(MetadataFileUploadResource.class);

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @RequestMapping(value = "/modules/{moduleName}/{metadataType}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload a metadata file")
    public ResponseDocument<String> uploadFile(@PathVariable String metadataType, //
            @PathVariable String moduleName, //
            @RequestParam("artifactName") String artifactName, //
            @RequestParam("metadataFile") MultipartFile metadataFile) {
        try {
            return ResponseDocument.successResponse(metadataFileUploadService.uploadFile(metadataType, //
                    moduleName, artifactName, // 
                    metadataFile.getInputStream()));
        } catch (Exception e) {
            if (e instanceof LedpException) {
                throw (LedpException) e;
            }
            log.error(e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/modules", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of modules")
    public List<Module> getModules() {
        return metadataFileUploadService.getModules();
    }

}
