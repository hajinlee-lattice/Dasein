package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.entitymgr.EnrichmentTemplateEntityMgr;
import com.latticeengines.apps.dcp.service.EnrichmentTemplateService;
import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Service("enrichmentTemplateService")
public class EnrichmentTemplateServiceImpl implements EnrichmentTemplateService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTemplateServiceImpl.class);

    @Inject
    private EnrichmentTemplateEntityMgr enrichmentTemplateEntityMgr;

    @Inject
    private EnrichmentLayoutEntityMgr enrichmentLayoutEntityMgr;

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Inject
    private EntitlementService entitlementService;

    @Inject
    private TenantService tenantService;

    @Override
    public ResponseDocument<String> create(String layoutId, String templateName) {
        Tenant tenant = tenantService
                .findByTenantId(CustomerSpace.parse(MultiTenantContext.getShortTenantId()).toString());
        EnrichmentLayout enrichmentLayout = enrichmentLayoutEntityMgr.findByLayoutId(layoutId);

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
                result = new ResponseDocument<>();
                List<String> errors = new ArrayList<>();
                errors.add(String.format("Error creating enrichment template %s, error message %s",
                        enrichmentTemplate.getTemplateId(), exception.getMessage()));
                result.setErrors(errors);
            }
        }
        return result;
    }

    private ResponseDocument<String> validateEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate) {
        ResponseDocument<String> result;
        if (enrichmentTemplate.getDomain() == null || enrichmentTemplate.getRecordType() == null
                || enrichmentTemplate.getTenant() == null) {
            List<String> errors = new ArrayList<>();
            if (enrichmentTemplate.getDomain() == null) {
                errors.add("Required field Domain is null");
            }
            if (enrichmentTemplate.getRecordType() == null) {
                errors.add("Required field RecordType is null");
            }
            if (enrichmentTemplate.getTenant() == null) {
                errors.add("Required field Tenant is null");
            }
            if (enrichmentTemplate.getCreatedBy() == null) {
                errors.add("Required creator is null");
            }
            result = new ResponseDocument<>();
            result.setErrors(errors);
        } else {
            String tenantId = enrichmentTemplate.getTenant().getId();
            DataBlockEntitlementContainer dataBlockEntitlementContainer = entitlementService.getEntitlement(tenantId,
                    "ALL", "ALL");
            result = validateDomain(enrichmentTemplate, dataBlockEntitlementContainer);
        }
        return result;
    }

    private ResponseDocument<String> validateDataRecordType(EnrichmentTemplate enrichmentTemplate,
            Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> map) {
        List<String> errors = new ArrayList<>();
        // Get set of blockIds/levels that the tenant must have for the template to be
        // valid
        Collection<String> blocksContainingElements = primeMetadataProxy
                .getBlocksContainingElements(enrichmentTemplate.getElements());

        // Get list of dataBlocks available for hte dataRecordType in this tenant.
        // This is used to determine if the required blocks are present.
        DataRecordType dataRecordType = enrichmentTemplate.getRecordType();
        List<DataBlockEntitlementContainer.Block> dataBlockList = map.get(dataRecordType);

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
                    errors.add(err);
                }
            }
        } else {
            errors.add(String.format("Data Record Type %s does not contain any data blocks.", dataRecordType.name()));
        }
        if (errors.isEmpty()) {
            return ResponseDocument.successResponse("success");
        } else {
            ResponseDocument<String> responseDocument = new ResponseDocument<>();
            responseDocument.setErrors(errors);
            return responseDocument;
        }
    }

    private ResponseDocument<String> validateDomain(EnrichmentTemplate enrichmentTemplate,
            DataBlockEntitlementContainer dataBlockEntitlementContainer) {
        // Get list of domains for this tenant
        List<DataBlockEntitlementContainer.Domain> domains = dataBlockEntitlementContainer.getDomains();

        // Check that EnrichmentTemplate domain is in list of tenant domains
        for (DataBlockEntitlementContainer.Domain domain : domains) {
            if (domain.getDomain() == enrichmentTemplate.getDomain()) {
                return validateDataRecordType(enrichmentTemplate, domain.getRecordTypes());
            }
        }
        // If it wasn't, template is not valid.
        ResponseDocument<String> response = new ResponseDocument<>();
        response.setErrors(Collections.singletonList(
                String.format("Enrichment template is not valid. %s domain is not valid domain for this user.",
                        enrichmentTemplate.getDomain().name())));
        return response;
    }
}
