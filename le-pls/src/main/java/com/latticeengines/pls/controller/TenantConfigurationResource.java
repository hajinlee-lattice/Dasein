package com.latticeengines.pls.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantConfiguration;
import com.latticeengines.pls.service.TenantConfigService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Tenant config", description = "REST resource for tenant config")
@RestController
@RequestMapping(value = "/tenantconfig")
public class TenantConfigurationResource {

    private static final Logger log = LoggerFactory.getLogger(TenantConfigurationResource.class);

    @Autowired
    private TenantConfigService configService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
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
