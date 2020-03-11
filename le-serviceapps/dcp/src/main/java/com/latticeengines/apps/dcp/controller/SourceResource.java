package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Source", description = "REST resource for source")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/source")
public class SourceResource {

    private static final Logger log = LoggerFactory.getLogger(SourceResource.class);

    @Inject
    private SourceService sourceService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a Source")
    public Source createSource(@PathVariable String customerSpace, @RequestBody SourceRequest sourceRequest) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (sourceRequest == null) {
            log.error("Create Source with empty SourceRequest!");
            throw new RuntimeException("Cannot create source with empty create source request input!");
        }
        if (StringUtils.isBlank(sourceRequest.getSourceId())) {
            log.debug("Create source with empty sourceId.");
            return sourceService.createSource(customerSpace, sourceRequest.getDisplayName(),
                    sourceRequest.getProjectId(), sourceRequest.getFieldDefinitionsRecord());
        } else {
            log.debug("Create source with specified sourceId: " + sourceRequest.getSourceId());
            return sourceService.createSource(customerSpace, sourceRequest.getDisplayName(),
                    sourceRequest.getProjectId(), sourceRequest.getSourceId(),
                    sourceRequest.getFieldDefinitionsRecord());
        }
    }

    @GetMapping("/sourceId/{sourceId}")
    @ApiOperation(value = "Get a Source")
    @ResponseBody
    public Source getSource(@PathVariable String customerSpace, @PathVariable String sourceId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return sourceService.getSource(customerSpace, sourceId);
    }

    @GetMapping("/projectId/{projectId}")
    @ResponseBody
    @ApiOperation(value = "Get all Sources in Project")
    public List<Source> getSourceUnderProject(@PathVariable String customerSpace, @PathVariable String projectId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return sourceService.getSourceList(customerSpace, projectId);
    }

    @DeleteMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Delete source by sourceId")
    public Boolean deleteSource(@PathVariable String customerSpace, @PathVariable String sourceId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return sourceService.deleteSource(customerSpace, sourceId);
    }

}
