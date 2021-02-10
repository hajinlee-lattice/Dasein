package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.CreateEnrichmentTemplateRequest;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;
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

    @PostMapping("/layout")
    @ResponseBody
    @ApiOperation(value = "Create an EnrichmentTemplate from Layout")
    public ResponseDocument<EnrichmentTemplateSummary> create(@PathVariable String customerSpace,
            @RequestBody CreateEnrichmentTemplateRequest request) {
        try {
            return ResponseDocument.successResponse(enrichmentTemplateService.create(customerSpace, request));
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template from existing layout %s: %s",
                    request.getLayoutId(), exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            return ResponseDocument.failedResponse(exception);
        }
    }

    @PostMapping("/create-template")
    @ResponseBody
    @ApiOperation(value = "Create an Enrichment Template")
    public ResponseDocument<EnrichmentTemplateSummary> create(@PathVariable String customerSpace,
            @RequestBody EnrichmentTemplate template) {
        try {
            return ResponseDocument.successResponse(enrichmentTemplateService.create(template));
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template: template ID %s", template.getTemplateId(),
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            return ResponseDocument.failedResponse(exception);
        }
    }

    @PostMapping("/list")
    @ResponseBody
    @ApiOperation(value = "List Enrichment Templates")
    public List<EnrichmentTemplateSummary> listEnrichmentTemplates(@PathVariable String customerSpace,
            @RequestBody ListEnrichmentTemplateRequest listEnrichmentTemplateRequest) {
        return enrichmentTemplateService.listEnrichmentTemplates(listEnrichmentTemplateRequest);
    }

    @GetMapping("/template/{templateId}")
    @ResponseBody
    @ApiOperation(value = "Get Enrichment Template")
    public EnrichmentTemplateSummary getEnrichmentTemplate(@PathVariable String customerSpace, @PathVariable String templateId) {
        return enrichmentTemplateService.getEnrichmentTemplate(templateId);
    }
}
