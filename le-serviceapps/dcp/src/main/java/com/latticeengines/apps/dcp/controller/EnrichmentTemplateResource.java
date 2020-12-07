package com.latticeengines.apps.dcp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "EnrichmentTemplate")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/enrichmenttemplate")
public class EnrichmentTemplateResource {

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @PostMapping("/{layoutId}/{templateName}")
    @ResponseBody
    @ApiOperation(value = "Create an EnrichmentTemplate from Layout")
    public EnrichmentTemplate create(@PathVariable String layoutId, @PathVariable String templateName) {
        return enrichmentTemplateService.create(layoutId, templateName);
    }
}
