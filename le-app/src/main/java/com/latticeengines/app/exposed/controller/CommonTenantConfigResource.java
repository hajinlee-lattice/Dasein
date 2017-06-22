package com.latticeengines.app.exposed.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Tenant config", description = "REST resource for tenant config")
@RestController
@RequestMapping(value = "/tenant")
public class CommonTenantConfigResource {

    @Autowired
    private CommonTenantConfigService configService;

    @RequestMapping(value = "/featureflags", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant's feature flags")
    public FeatureFlagValueMap getFeatureFlags() {
        return configService.getFeatureFlags(MultiTenantContext.getTenant().getId());
    }
}
