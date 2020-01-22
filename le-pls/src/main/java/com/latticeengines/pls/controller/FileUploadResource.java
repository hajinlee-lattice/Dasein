package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.pls.service.FileUploadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "fileuploads", description = "REST resource for obtaining general information about uploaded files")
@RestController
@RequestMapping("/fileuploads")
@PreAuthorize("hasRole('View_PLS_Data')")
public class FileUploadResource {
    @Inject
    private FileUploadService fileUploadService;

    @RequestMapping(value = "{fileName}/import/errors", method = RequestMethod.GET, produces = "application/csv")
    @ResponseBody
    @ApiOperation(value = "Retrieve file import errors")
    public void getImportErrors(@PathVariable String fileName, HttpServletResponse response) {
        try {
            InputStream is = fileUploadService.getImportErrorStream(fileName);
            response.setContentType("application/csv");
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", "errors.csv"));
            IOUtils.copy(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18093, e);
        }
    }
    
    @RequestMapping(value = "{fileName}/metadata", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve metadata for a source file")
    public Table getMetadataForSourceFile(@PathVariable String fileName) {
        return fileUploadService.getMetadata(fileName);
    }

}
