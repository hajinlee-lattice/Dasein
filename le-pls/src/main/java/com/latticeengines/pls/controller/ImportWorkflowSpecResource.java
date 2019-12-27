package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "specs", description = "REST resource for operations on import workflow spec")
@RestController
@RequestMapping("/specs")
@PreAuthorize("hasRole('View_PLS_Spec')")
public class ImportWorkflowSpecResource {

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    @Inject
    private DataFileProviderService dataFileProviderService;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    // API to upload a new Spec
    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "upload a new Spec to S3")
    public String uploadSpecToS3(
            @RequestParam(value = "systemType") String systemType, //
            @RequestParam(value = "systemObject") String systemObject, //
            @RequestParam("file") MultipartFile file) {
        try {
            InputStream stream = file.getInputStream();
            return modelingFileMetadataService.uploadIndividualSpec(systemType, systemObject, stream);
        } catch (Exception e) {
            return null;
        }
    }

    // API to list specs by systemType and systemObject
    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "list the specs in system")
    public List<ImportWorkflowSpec> listSpecs(
            @RequestParam(value = "systemType", required = false) String systemType, //
            @RequestParam(value = "systemObject", required = false) String systemObject) {

        String customerSpace = MultiTenantContext.getShortTenantId();
        return importWorkflowSpecProxy.getSpecsByTypeAndObject(customerSpace, systemType, systemObject);
    }

    // API to download a spec
    @RequestMapping(value = "/download", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get spec info from s3")
    public void downloadSpecFromS3(
            @RequestParam(value = "systemType") String systemType,
            @RequestParam(value = "systemObject") String systemObject,
            HttpServletRequest request, //
            HttpServletResponse response) throws IOException {
        try {
            dataFileProviderService.downloadSpecFromS3(request, response, MediaType.APPLICATION_JSON, systemType, systemObject);
        } catch(Exception e) {
            throw e;
        }
    }
}
