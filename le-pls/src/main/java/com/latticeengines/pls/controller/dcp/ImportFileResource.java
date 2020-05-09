package com.latticeengines.pls.controller.dcp;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.pls.service.dcp.SourceFileUploadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "importfile", description = "REST resource for import files for modeling")
@RestController
@RequestMapping("/importfile")
@PreAuthorize("hasRole('View_DCP_Projects')")
public class ImportFileResource {

    private static final Logger log = LoggerFactory.getLogger(ImportFileResource.class);

    @Inject
    private SourceFileUploadService sourceFileUploadService;


    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public SourceFileInfo uploadFile(
            @RequestParam("name") String csvFileName,
            @RequestParam("file") MultipartFile file) {
        return sourceFileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv", csvFileName, false, null, file);
    }

}
