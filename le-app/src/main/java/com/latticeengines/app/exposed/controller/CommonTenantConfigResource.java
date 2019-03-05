package com.latticeengines.app.exposed.controller;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Deprecated
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

    @RequestMapping(value = "/products", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant's feature flags")
    public List<String> getProducts() {
        List<LatticeProduct> products = configService.getProducts(MultiTenantContext.getTenant().getId());
        if (products != null) {
            return products.stream().map(LatticeProduct::getName).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
