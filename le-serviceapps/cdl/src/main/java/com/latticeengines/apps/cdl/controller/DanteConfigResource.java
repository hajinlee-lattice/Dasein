package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DanteConfigService;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

import io.swagger.annotations.Api;


@Api(value = "DanteConfigurationDocument", description = "REST resource for dante configs.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/danteconfig")
public class DanteConfigResource {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigResource.class);

    @Inject
    private DanteConfigService danteConfigService;


    @GetMapping("/getDanteConfiguration")
    public DanteConfigurationDocument getDanteConfiguration(@PathVariable String customerSpace){
        return danteConfigService.getDanteConfiguration();
    }
}
