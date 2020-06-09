package com.latticeengines.app.exposed.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantConfiguration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Tenant config", description = "REST resource for tenant config")
@RestController
@RequestMapping("/tenantconfig")
public class TenantConfigurationResource {

    private static final Logger log = LoggerFactory.getLogger(TenantConfigurationResource.class);

    @Inject
    private CommonTenantConfigService configService;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get tenant's configuration")
    public TenantConfiguration getFeatureFlags() {
        try {
            return configService.getTenantConfiguration();
        } catch (Exception e) {
            log.error("Failed to get tenant configuration.", e);
            throw new LedpException(LedpCode.LEDP_18150, e);
        }
    }

}
