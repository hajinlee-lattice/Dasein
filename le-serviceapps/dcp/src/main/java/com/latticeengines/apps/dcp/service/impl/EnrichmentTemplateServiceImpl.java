package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.entitymgr.EnrichmentTemplateEntityMgr;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

@Service("enrichmentTemplateService")
public class EnrichmentTemplateServiceImpl extends ServiceCommonImpl implements EnrichmentTemplateService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTemplateServiceImpl.class);
    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_RETRY = 3;

    @Inject
    private EnrichmentTemplateEntityMgr enrichmentTemplateEntityMgr;

    @Inject
    private EnrichmentLayoutEntityMgr enrichmentLayoutEntityMgr;

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Inject
    private EntitlementService entitlementService;

    @Override
    public EnrichmentTemplateSummary create(String customerSpace, String layoutId, String templateName) {
        Tenant tenant = MultiTenantContext.getTenant();
        EnrichmentLayout enrichmentLayout = enrichmentLayoutService.findByLayoutId(customerSpace, layoutId);

        if (enrichmentLayout == null) {
            log.error("Could not find an enrichment layout with layoutId " + layoutId);
            throw new LedpException(LedpCode.LEDP_60014, new String[] { layoutId });
        }

        EnrichmentTemplate enrichmentTemplate = new EnrichmentTemplate(enrichmentLayout);

        enrichmentTemplate.setCreatedBy(MultiTenantContext.getUser().getEmail());
        enrichmentTemplate.setTemplateName(templateName);
        enrichmentTemplate.setTenant(tenant);

        ResponseDocument<String> result = validateEnrichmentTemplate(enrichmentTemplate);
        if (result.isSuccess()) {
            try {
                enrichmentTemplateEntityMgr.create(enrichmentTemplate);
                enrichmentLayout.setTemplateId(enrichmentTemplate.getTemplateId());
                enrichmentLayoutEntityMgr.update(enrichmentLayout);
            } catch (Exception exception) {
                log.error(String.format("Error creating enrichment template %s, error message %s",
                        enrichmentTemplate.getTemplateId(), exception.getMessage()));
                throw new LedpException(LedpCode.LEDP_60015,
                        new String[] { enrichmentTemplate.getTemplateId(), exception.getMessage() });
            }
            return new EnrichmentTemplateSummary(enrichmentTemplate);
        } else {
            throw new LedpException(LedpCode.LEDP_60016, new String[] { String.join("\n", result.getErrors()) });
        }
    }

    @Override
    public EnrichmentTemplateSummary create(EnrichmentTemplate enrichmentTemplate) {
        Tenant tenant = MultiTenantContext.getTenant();
        enrichmentTemplate.setTenant(tenant);

        ResponseDocument<String> result = validateEnrichmentTemplate(enrichmentTemplate);
        if (result.isSuccess()) {
            try {
                enrichmentTemplateEntityMgr.create(enrichmentTemplate);
            } catch (Exception exception) {
                log.error(String.format("Error creating enrichment template %s, error message %s",
                        enrichmentTemplate.getTemplateId(), exception.getMessage()));
                throw new LedpException(LedpCode.LEDP_60015,
                        new String[] { enrichmentTemplate.getTemplateId(), exception.getMessage() });
            }
            return new EnrichmentTemplateSummary(enrichmentTemplate);
        } else {
            throw new LedpException(LedpCode.LEDP_60016, new String[] { String.join("\n", result.getErrors()) });
        }
    }

    @Override
    public List<EnrichmentTemplateSummary> listEnrichmentTemplates(
            ListEnrichmentTemplateRequest listEnrichmentTemplateRequest) {
        PageRequest pageRequest = getPageRequest(0, MAX_PAGE_SIZE);
        try {
            List<EnrichmentTemplate> templates = enrichmentTemplateEntityMgr.findAll(pageRequest);

            templates = templates.stream().filter(et -> includeEnrichmentTemplate(listEnrichmentTemplateRequest, et))
                    .collect(Collectors.toList());

            return templates.stream().map(EnrichmentTemplateSummary::new).collect(Collectors.toList());
        } catch (Exception exception) {
            log.error(ExceptionUtils.getStackTrace(exception));
            log.error(String.format("Error querying for enrichment templates: %s", exception.getMessage()));
            throw new LedpException(LedpCode.LEDP_60017, new String[] { exception.getMessage() });
        }
    }

    @Override
    public EnrichmentTemplateSummary getEnrichmentTemplate(String templateId) {
        EnrichmentTemplate template = enrichmentTemplateEntityMgr.find(templateId);
        return template == null ? null : new EnrichmentTemplateSummary(template);
    }

    private boolean includeEnrichmentTemplate(ListEnrichmentTemplateRequest listEnrichmentTemplateRequest,
            EnrichmentTemplate enrichmentTemplate) {
        boolean matchesDomain = "ALL".equals(listEnrichmentTemplateRequest.getDomain())
                || listEnrichmentTemplateRequest.getDomain().equals(enrichmentTemplate.getDomain().name());

        boolean matchesRecordType = "ALL".equals(listEnrichmentTemplateRequest.getRecordType())
                || listEnrichmentTemplateRequest.getRecordType().equals(enrichmentTemplate.getRecordType().name());

        boolean matchesCreatedBy = "ALL".equals(listEnrichmentTemplateRequest.getCreatedBy())
                || listEnrichmentTemplateRequest.getCreatedBy().equals(enrichmentTemplate.getCreatedBy());

        boolean matchesArchived = listEnrichmentTemplateRequest.getIncludeArchived()
                || !enrichmentTemplate.getArchived();

        return (matchesDomain && matchesRecordType && matchesCreatedBy && matchesArchived);
    }

    private ResponseDocument<String> validateEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate) {
        ResponseDocument<String> result;
        List<String> errors = new ArrayList<>();

        if (enrichmentTemplate.getDomain() == null) {
            String msg = "Required field Domain is null";
            errors.add(msg);
            log.warn(msg);
        }
        if (enrichmentTemplate.getRecordType() == null) {
            String msg = "Required field RecordType is null";
            errors.add(msg);
            log.warn(msg);
        }
        if (enrichmentTemplate.getTenant() == null) {
            String msg = "Required field Tenant is null";
            errors.add(msg);
            log.warn(msg);
        }
        if (enrichmentTemplate.getCreatedBy() == null) {
            String msg = "Required creator is null";
            errors.add(msg);
            log.warn(msg);
        }

        if (!errors.isEmpty()) {
            result = new ResponseDocument<>();
            result.setErrors(errors);
        } else {
            result = validateDomain(enrichmentTemplate);
        }
        return result;
    }

    private ResponseDocument<String> validateDataRecordType(EnrichmentTemplate enrichmentTemplate,
            List<DataBlockEntitlementContainer.Block> dataBlockList) {
        List<String> errors = new ArrayList<>();
        // Get set of blockIds/levels that the tenant must have for the template to be
        // valid
        Collection<String> blocksContainingElements = primeMetadataProxy
                .getBlocksContainingElements(enrichmentTemplate.getElements());

        if (dataBlockList != null) {
            // Build a set of authorized data blocks and levels
            Set<String> authorizedElements = new HashSet<>();
            for (DataBlockEntitlementContainer.Block block : dataBlockList) {
                for (DataBlockLevel level : block.getDataBlockLevels()) {
                    String element = block.getBlockId() + "_" + level.name();
                    authorizedElements.add(element);
                }
            }

            // Iterate through blocks and levels the the template needs, and make sure they
            // are available for the tenant
            for (String neededElement : blocksContainingElements) {
                // Trim out version number to standardize element format
                String checkingString = neededElement.substring(0, neededElement.lastIndexOf("_"));
                if (!authorizedElements.contains(checkingString)) {
                    // Unauthorized element, template is not valid.
                    String err = String.format(
                            "Enrichment template is not valid, element %s is not authorized for subscriber number %s.",
                            neededElement, enrichmentTemplate.getTenant().getSubscriberNumber());
                    log.error(err);
                    errors.add(err);
                }
            }
        } else {
            String err = String.format("Data Record Type does not contain any data blocks.");
            log.error(err);
            errors.add(err);
        }
        if (errors.isEmpty()) {
            return ResponseDocument.successResponse("success");
        } else {
            ResponseDocument<String> responseDocument = new ResponseDocument<>();
            responseDocument.setErrors(errors);
            return responseDocument;
        }
    }

    private ResponseDocument<String> validateDomain(EnrichmentTemplate enrichmentTemplate) {
        String tenantId = enrichmentTemplate.getTenant().getId();
        DataBlockEntitlementContainer dataBlockEntitlementContainer;
        ResponseDocument<String> response;

        try {
            dataBlockEntitlementContainer = entitlementService.getEntitlement(tenantId, "ALL", "ALL");
        } catch (Exception e) {
            log.error(String.format("Unexpected error getting tenant entitlements: Tenant %s:\n%s", tenantId,
                    e.getMessage()));
            dataBlockEntitlementContainer = null;
        }

        if (dataBlockEntitlementContainer != null) {
            // Get list of domains for this tenant
            List<DataBlockEntitlementContainer.Domain> domains = dataBlockEntitlementContainer.getDomains();

            // Check that EnrichmentTemplate domain is in list of tenant domains
            for (DataBlockEntitlementContainer.Domain domain : domains) {
                if (domain.getDomain() == enrichmentTemplate.getDomain()) {
                    return validateDataRecordType(enrichmentTemplate,
                            domain.getRecordTypes().get(enrichmentTemplate.getRecordType()));
                }
            }
            // If it wasn't, template is not valid.
            response = new ResponseDocument<>();
            response.setErrors(Collections.singletonList(
                    String.format("Enrichment template is not valid. %s domain is not valid domain for this user.",
                            enrichmentTemplate.getDomain().name())));
        } else {
            response = new ResponseDocument<>();
            response.setErrors(Collections.singletonList(String.format(
                    "Could not validate entitlements while creating enrichment template for tenant %s", tenantId)));
        }
        return response;
    }
}
