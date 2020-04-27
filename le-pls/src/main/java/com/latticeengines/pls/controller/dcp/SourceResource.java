package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
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

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;


@Api(value = "Sources")
@RestController
@RequestMapping("/sources")
@PreAuthorize("hasRole('View_DCP_Projects')")
public class SourceResource {

    private static final Logger log = LoggerFactory.getLogger(SourceResource.class);

    @Inject
    private SourceService sourceService;

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    private ModelingFileMetadataService modelingFileMetadataService;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation("Create source")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public Source createSource(@RequestBody SourceRequest sourceRequest) {
        try {
            return sourceService.createSource(sourceRequest);
        } catch (RuntimeException e) {
            log.error("Failed to create source: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, LedpCode.LEDP_60001);
        }
    }

    @GetMapping(value = "/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get sources by sourceId")
    public Source getSource(@PathVariable String sourceId) {
        return sourceService.getSource(sourceId);
    }

    @DeleteMapping(value = "/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Delete source by sourceId")
    public Boolean deleteSource(@PathVariable String sourceId) {
        return sourceService.deleteSource(sourceId);
    }


    @GetMapping(value = "/projectId/{projectId}")
    @ResponseBody
    @ApiOperation("Get sources by projectId")
    public List<Source> getSourceUnderProduct(@PathVariable String projectId) {
        return sourceService.getSourceList(projectId);
    }

    @PutMapping(value = "/sourceId/{sourceId}/pause")
    @ResponseBody
    @ApiOperation("Pause source by sourceId")
    public Boolean pauseSource(@PathVariable String sourceId) {
        return sourceService.pauseSource(sourceId);
    }


    // Parameters:
    //   systemObject: The entity type of this template (also called EntityType.displayName), eg. Accounts
    //   importFile: The name of the CSV file this template is being generated for.
    @RequestMapping(value = "/fetch", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Provide field definition to Front End so it can load page of import workflow")
    public FetchFieldDefinitionsResponse fetchFieldDefinitions(
            @RequestParam(value = "sourceId", required = false) String sourceId, //
            @RequestParam(value = "systemObject", required = false, defaultValue = "Accounts") String systemObject, //
            @RequestParam(value = "importFile") String importFile) {
        try {
            return sourceService.fetchFieldDefinitions(sourceId, systemObject, importFile);
        } catch (Exception e) {
            log.error("Fetch Field Definition Failed with Exception: ", e);
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, LedpCode.LEDP_60002);
        }
    }

    // Parameters:
    //   importFile: The name of the CSV file this template is being generated for.
    // Body:
    // ValidateFieldDefinitionsRequest representing field definition changes/records
    @RequestMapping(value = "/validate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Provide validation result and merged field definition to front end")
    public ValidateFieldDefinitionsResponse validateFieldDefinitions(
            @RequestParam(value = "importFile") String importFile, //
            @RequestBody ValidateFieldDefinitionsRequest validateRequest) {
        ValidateFieldDefinitionsResponse validateFieldDefinitionsResponse = null;
        try {
            validateFieldDefinitionsResponse = sourceService.validateFieldDefinitions(
                    importFile, validateRequest);
            return validateFieldDefinitionsResponse;
        } catch (Exception e) {
            log.error("Failed to create source: " + e.getMessage());
            UIAction action = graphDependencyToUIActionUtil.generateUIAction("", View.Banner,
                    Status.Error, e.getMessage());
            throw new UIActionException(action, LedpCode.LEDP_60003);
        }
    }
}
