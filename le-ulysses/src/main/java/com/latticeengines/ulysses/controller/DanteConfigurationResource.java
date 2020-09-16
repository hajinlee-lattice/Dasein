package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
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

@Api(value = "DanteConfiguration", description = "Common REST resource to serve configuration for Dante UI")
@RestController
@RequestMapping("/danteconfiguration")
public class DanteConfigurationResource {
    private static final Logger log = LoggerFactory.getLogger(DanteConfigurationResource.class);

    @Inject
    CDLDanteConfigProxy cdlDanteConfigProxy;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get Dante configuration")
    public FrontEndResponse<DanteConfigurationDocument> getDanteConfiguration() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        try {
            PerformanceTimer timer = new PerformanceTimer("get Dante Configuration", log);
            DanteConfigurationDocument danteConfigurationDocument = cdlDanteConfigProxy.getDanteConfiguration(customerSpace);
            timer.close();
            return new FrontEndResponse<>(danteConfigurationDocument);
        } catch (LedpException le) {
            log.error("Failed to get talking point data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get talking point data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }
}
