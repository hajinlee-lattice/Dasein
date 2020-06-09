package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.metadata.service.ArtifactService;
import com.latticeengines.metadata.service.ArtifactValidationService;
import com.latticeengines.metadata.validation.service.impl.ArtifactValidation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "REST resource for metadata artifacts")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ArtifactResource {

    @Inject
    private ArtifactService artifactService;

    @PostMapping("/modules/{moduleName}/artifacts/{artifactName}")
    @ResponseBody
    @ApiOperation(value = "Create artifact")
    public Boolean createArtifact(@PathVariable String customerSpace, //
            @PathVariable String moduleName, //
            @PathVariable String artifactName, //
            @RequestBody Artifact artifact) {
        artifactService.createArtifact(customerSpace, moduleName, artifactName, artifact);
        return true;
    }

    @PostMapping("/artifacttype/{artifactType}")
    @ResponseBody
    @ApiOperation(value = "Validate artifact file")
    public ResponseDocument<String> validateArtifact(@PathVariable ArtifactType artifactType, //
            @RequestParam("file") String artifactFilePath) {
        ArtifactValidationService artifactValidationService = ArtifactValidation
                .getArtifactValidationService(artifactType);
        if (artifactValidationService == null) {
            return ResponseDocument.successResponse("");
        }
        artifactValidationService.validate(artifactFilePath);
        return ResponseDocument.successResponse("");
    }

    @GetMapping("/artifactpath")
    @ResponseBody
    @ApiOperation(value = "Get Artifact By Path")
    public Artifact getArtifactByPath(@PathVariable String customerSpace, //
            @RequestParam("file") String artifactPath) {
        return artifactService.getArtifactByPath(customerSpace, artifactPath);
    }
}
