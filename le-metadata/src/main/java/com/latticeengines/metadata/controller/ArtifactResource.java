package com.latticeengines.metadata.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.metadata.service.ArtifactService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata artifacts")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ArtifactResource {

    @Autowired
    private ArtifactService artifactService;
    
    @RequestMapping(value = "/modules/{moduleName}/artifacts/{artifactName}", //
                    method = RequestMethod.POST, //
                    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create artifact")
    public Boolean createArtifact(@PathVariable String customerSpace, //
                                   @PathVariable String moduleName, //
                                   @PathVariable String artifactName, //
                                   @RequestBody Artifact artifact, //
                                   HttpServletRequest request) {
        artifactService.createArtifact(customerSpace, moduleName, artifactName, artifact);
        return true;
    }
    
    @RequestMapping(value = "/modules/{moduleName}", //
                    method = RequestMethod.GET, //
                    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of artifacts")
    public List<Artifact> getArtifacts(@PathVariable String customerSpace, //
                                        @PathVariable String moduleName, //
                                        HttpServletRequest request) {
        return artifactService.findAll(customerSpace, moduleName);
    }
}
