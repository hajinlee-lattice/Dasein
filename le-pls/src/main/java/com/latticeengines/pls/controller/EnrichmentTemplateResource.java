package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.Status;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
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
    public ResponseDocument<String> createTemplate(@PathVariable String layoutId, @RequestParam String templateName) {
        try {
            ResponseDocument<String> createTemplateResponse = enrichmentTemplateService
                    .createEnrichmentTemplate(layoutId, templateName);
            return createTemplateResponse;
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template from existing layout %s: %s", layoutId,
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            UIAction action = UIActionUtils.generateUIAction("", View.Banner, Status.Error, exception.getMessage());
            throw new UIActionException(action, exception.getCode());
        }
    }

    @PostMapping(value = "/create-template")
    @ResponseBody
    @ApiOperation("Create enrichment template")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public ResponseDocument<String> createTemplate(@RequestBody EnrichmentTemplate template) {
        try {
            ResponseDocument<String> createTemplateResponse = enrichmentTemplateService
                    .createEnrichmentTemplate(template);
            return createTemplateResponse;
        } catch (LedpException exception) {
            log.error(String.format("Failed to create enrichment template: template ID %s", template.getTemplateId(),
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            UIAction action = UIActionUtils.generateUIAction("", View.Banner, Status.Error, exception.getMessage());
            throw new UIActionException(action, exception.getCode());
        }
    }

    @GetMapping("/list")
    @ResponseBody
    @ApiOperation("List enrichment templates")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public List<EnrichmentTemplateSummary> getTemplates(
            @RequestParam(value = "domain", required = false, defaultValue = "ALL") String domain,
            @RequestParam(value = "recordType", required = false, defaultValue = "ALL") String recordType,
            @RequestParam(value = "includeArchived", required = false, defaultValue = "false") boolean includeArchived,
            @RequestParam(value = "createdBy", required = false, defaultValue = "ALL") String createdBy) {
        String tenantId = MultiTenantContext.getShortTenantId();
        try {
            return enrichmentTemplateService.getEnrichmentTemplates(domain, recordType, includeArchived, createdBy);
        } catch (LedpException exception) {
            log.error(String.format("Failed to get list of enrichment templates for tenant %s", tenantId,
                    exception.getMessage()));
            log.error(ExceptionUtils.getStackTrace(exception));
            UIAction action = UIActionUtils.generateUIAction("", View.Banner, Status.Error, exception.getMessage());
            throw new UIActionException(action, exception.getCode());
        }
    }
}
