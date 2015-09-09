package com.latticeengines.admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "featureflagsadmin", description = "REST resource for managing feature flags")
@RestController
@RequestMapping(value = "/featureflags")
@PostAuthorize("hasRole('Platform Operations') or hasRole('DeveloperSupport')")
public class FeatureFlagResource {

    @Autowired
    private FeatureFlagService featureFlagService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all feature flag definitions")
    public FeatureFlagDefinitionMap getAllDefinitions() {
        return featureFlagService.getDefinitions();
    }

    @RequestMapping(value = "/{flagId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a feature flag")
    public SimpleBooleanResponse defineFeatureFlag(@PathVariable String flagId, //
                                                   @RequestBody FeatureFlagDefinition definition) {
        featureFlagService.defineFlag(flagId, definition);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{flagId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove a feature flag definition")
    public SimpleBooleanResponse removeFlagDefinition(@PathVariable String flagId) {
        featureFlagService.undefineFlag(flagId);
        return SimpleBooleanResponse.successResponse();
    }
}
