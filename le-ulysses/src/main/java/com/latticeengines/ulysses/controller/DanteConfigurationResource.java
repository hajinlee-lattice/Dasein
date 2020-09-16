package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.proxy.exposed.cdl.CDLDanteConfigProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "DanteConfiguration")
@RestController
@RequestMapping("/danteconfiguration")
public class DanteConfigurationResource {
    private static final Logger log = LoggerFactory.getLogger(DanteConfigurationResource.class);

    @Inject
    CDLDanteConfigProxy cdlDanteConfigProxy;

    @GetMapping("/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Get a dante configuration by tenantId")
    public DanteConfigurationDocument getDanteconfigurationByTenantId(@PathVariable String tenantId) {
        return cdlDanteConfigProxy.getDanteConfiguration(tenantId);
    }

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get Dante configuration")
    public FrontEndResponse<DanteConfigurationDocument> getDanteConfiguration() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        try (PerformanceTimer timer = new PerformanceTimer("Get Dante Configuration | Tenant=" + customerSpace, log)) {
            DanteConfigurationDocument danteConfigurationDocument = getDanteconfigurationByTenantId(customerSpace);
            return new FrontEndResponse<>(danteConfigurationDocument);
        } catch (LedpException le) {
            log.error("Failed to get Dante Configuration document", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get Dante Configuration document", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }
}
