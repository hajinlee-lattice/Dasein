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
    public EnrichmentTemplate create(String layoutId, String templateName) {
        Tenant tenant = tenantService
                .findByTenantId(CustomerSpace.parse(MultiTenantContext.getShortTenantId()).toString());
        EnrichmentLayout enrichmentLayout = enrichmentLayoutEntityMgr.findByLayoutId(layoutId);

        if (enrichmentLayout == null) {
            log.error("Could not find an enrichment layout with layoutId " + layoutId);
            throw new LedpException(LedpCode.LEDP_60014, new String[] { layoutId });
        }

        EnrichmentTemplate enrichmentTemplate = new EnrichmentTemplate(enrichmentLayout);

        enrichmentTemplate.setTemplateName(templateName);
        enrichmentTemplate.setTenant(tenant);

        ResponseDocument<String> result = validateEnrichmentTemplate(enrichmentTemplate);
        if (result.isSuccess()) {
            enrichmentTemplateEntityMgr.create(enrichmentTemplate);
        }

        enrichmentLayout.setTemplateId(enrichmentTemplate.getTemplateId());
        enrichmentLayoutEntityMgr.update(enrichmentLayout);
        return enrichmentTemplate;
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
        Collection<String> blocksContainingElements = primeMetadataProxy
                .getBlocksContainingElements(enrichmentTemplate.getElements());
        DataRecordType dataRecordType = enrichmentTemplate.getRecordType();
        List<DataBlockEntitlementContainer.Block> dataBlockList = map.get(dataRecordType);

        if (dataBlockList != null) {
            Set<String> authorizedElements = new HashSet<>();
            for (DataBlockEntitlementContainer.Block block : dataBlockList) {
                for (DataBlockLevel level : block.getDataBlockLevels()) {
                    String element = block.getBlockId() + "_" + level.name();
                    authorizedElements.add(element);
                }
            }

            for (String neededElement : blocksContainingElements) {
                String checkingString = neededElement.substring(0, neededElement.lastIndexOf("_"));
                if (!authorizedElements.contains(checkingString)) {
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
        List<DataBlockEntitlementContainer.Domain> domains = dataBlockEntitlementContainer.getDomains();

        for (DataBlockEntitlementContainer.Domain domain : domains) {
            if (domain.getDomain() == enrichmentTemplate.getDomain()) {
                return validateDataRecordType(enrichmentTemplate, domain.getRecordTypes());
            }
        }
        ResponseDocument<String> response = new ResponseDocument<>();
        response.setErrors(Collections.singletonList(
                String.format("Enrichment template is not valid. %s domain is not valid domain for this user.",
                        enrichmentTemplate.getDomain().name())));
        return response;
    }
}
