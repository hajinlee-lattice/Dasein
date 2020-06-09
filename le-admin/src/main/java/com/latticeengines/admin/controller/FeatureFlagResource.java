package com.latticeengines.admin.controller;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "featureflagsadmin", description = "REST resource for managing feature flags")
@RestController
@RequestMapping("/featureflags")
@PostAuthorize("hasRole('adminconsole')")
public class FeatureFlagResource {

    @Inject
    private FeatureFlagService featureFlagService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all feature flag definitions")
    public FeatureFlagDefinitionMap getAllDefinitions() {
        return featureFlagService.getDefinitions();
    }

    @PostMapping("/{flagId}")
    @ResponseBody
    @ApiOperation(value = "Create a feature flag")
    public SimpleBooleanResponse defineFeatureFlag(@PathVariable String flagId, //
            @RequestBody FeatureFlagDefinition definition) {
        featureFlagService.defineFlag(flagId, definition);
        return SimpleBooleanResponse.successResponse();
    }

    @DeleteMapping("/{flagId}")
    @ResponseBody
    @ApiOperation(value = "Remove a feature flag definition")
    public SimpleBooleanResponse removeFlagDefinition(@PathVariable String flagId) {
        featureFlagService.undefineFlag(flagId);
        return SimpleBooleanResponse.successResponse();
    }
}
