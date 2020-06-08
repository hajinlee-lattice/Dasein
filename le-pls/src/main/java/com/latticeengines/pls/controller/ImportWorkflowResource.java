package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.pls.service.ImportWorkflowService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "importworkflow", description = "REST resource for operations on import workflow spec")
@RestController
@RequestMapping("/importworkflow")
@PreAuthorize("hasRole('View_PLS_Spec')")
public class ImportWorkflowResource {

    @Inject
    private ImportWorkflowService importWorkflowService;

    // API to upload a new Spec
    @PostMapping("/specs/upload")
    @ResponseBody
    @ApiOperation(value = "upload a new Spec to S3")
    public String uploadSpecToS3(
            @RequestParam(value = "systemType") String systemType, //
            @RequestParam(value = "systemObject") String systemObject, //
            @RequestParam("file") MultipartFile file) {
        try {
            InputStream stream = file.getInputStream();
            return importWorkflowService.uploadIndividualSpec(systemType, systemObject, stream);
        } catch (Exception e) {
            return null;
        }
    }

    // API to list specs by systemType and systemObject
    @GetMapping("/specs/list")
    @ResponseBody
    @ApiOperation(value = "list the specs in system")
    public List<ImportWorkflowSpec> listSpecs(
            @RequestParam(value = "systemType", required = false) String systemType, //
            @RequestParam(value = "systemObject", required = false) String systemObject) {

        String customerSpace = MultiTenantContext.getShortTenantId();
        return importWorkflowService.getSpecsByTypeAndObject(customerSpace, systemType, systemObject);
    }

    // API to download a spec
    @GetMapping("/specs/download")
    @ResponseBody
    @ApiOperation(value = "Get spec info from s3")
    public void downloadSpecFromS3(
            @RequestParam(value = "systemType") String systemType,
            @RequestParam(value = "systemObject") String systemObject,
            HttpServletRequest request, //
            HttpServletResponse response) throws IOException {
        try {
            importWorkflowService.downloadSpecFromS3(request, response, MediaType.APPLICATION_JSON, systemType, systemObject);
        } catch(Exception e) {
            throw e;
        }
    }
}
