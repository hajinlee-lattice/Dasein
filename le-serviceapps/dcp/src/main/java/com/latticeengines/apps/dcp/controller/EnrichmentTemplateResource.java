package com.latticeengines.apps.dcp.controller;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "EnrichmentTemplate")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/enrichmenttemplate")
public class EnrichmentTemplateResource {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTemplateResource.class);

    @Inject
    private EnrichmentTemplateService enrichmentTemplateService;

    @PostMapping("/{layoutId}")
    @ResponseBody
    @ApiOperation(value = "Create an EnrichmentTemplate from Layout")
    public ResponseDocument<String> create(@PathVariable String layoutId, @RequestBody String templateName) {
        try {
            ResponseDocument<String> result = enrichmentTemplateService.create(layoutId, templateName);
            return result;
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template from existing layout %s: %s", layoutId,
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            return ResponseDocument.failedResponse(exception);
        }
    }

    @PostMapping("/create-template")
    @ResponseBody
    @ApiOperation(value = "Create an Enrichment Template")
    public ResponseDocument<String> create(@RequestBody EnrichmentTemplate template) {
        try {
            ResponseDocument<String> result = enrichmentTemplateService.create(template);
            return result;
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template: template ID %s", template.getTemplateId(),
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            return ResponseDocument.failedResponse(exception);
        }
    }
}
