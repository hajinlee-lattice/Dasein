package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.pls.service.dcp.EnrichmentTemplateService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Enrichment Template")
@RestController
@RequestMapping("/enrichment-template")
public class EnrichmentTemplateResource {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTemplateResource.class);

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @PostMapping("/layout/{layoutId}")
    @ResponseBody
    @ApiOperation("Create enrichment template")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public ResponseDocument<String> createTemplate(@PathVariable String layoutId,
            @RequestParam(value = "templateName") String templateName) {
        return enrichmentTemplateService.createEnrichmentTemplate(MultiTenantContext.getShortTenantId(), layoutId,
                templateName);
    }
}
