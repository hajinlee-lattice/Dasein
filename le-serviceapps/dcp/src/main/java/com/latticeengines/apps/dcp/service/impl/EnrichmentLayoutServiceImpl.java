package com.latticeengines.apps.dcp.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Service("EnrichmentLayoutService")
public class EnrichmentLayoutServiceImpl extends ServiceCommonImpl implements EnrichmentLayoutService {

    @Inject
    private EnrichmentLayoutEntityMgr enrichmentLayoutEntityMgr;

    @Inject
    private AppendConfigService appendConfigService;

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Inject
    private TenantService tenantService;

    @Override
    public EnrichmentLayoutOperationResult create(String customerSpace, EnrichmentLayout enrichmentLayout) {
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        enrichmentLayout.setTenant(tenant);
        EnrichmentLayoutOperationResult result = validate(enrichmentLayout);
        if (result.isValid()) {
            enrichmentLayoutEntityMgr.create(enrichmentLayout);
        }
        return result;
    }

    @Override
    public List<EnrichmentLayoutDetail> getAll(String customerSpace, Boolean includeArchived, int pageIndex,
            int pageSize) {
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        return enrichmentLayoutEntityMgr.findAllEnrichmentLayoutDetail(pageRequest, includeArchived);
    }

    @Override
    public EnrichmentLayoutOperationResult update(String customerSpace, EnrichmentLayout enrichmentLayout) {
        EnrichmentLayoutOperationResult result = validate(enrichmentLayout);
        if (result.isValid()) {
            enrichmentLayoutEntityMgr.update(enrichmentLayout);
        }
        return result;
    }

    @Override
    public EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String customerSpace, String layoutId) {
        return enrichmentLayoutEntityMgr.findEnrichmentLayoutDetailByLayoutId(layoutId);
    }

    @Override
    public EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String customerSpace, String sourceId) {
        return enrichmentLayoutEntityMgr.findEnrichmentLayoutDetailBySourceId(sourceId);
    }

    @Override
    public EnrichmentLayout findByLayoutId(String customerSpace, String layoutId) {
        return enrichmentLayoutEntityMgr.findByField("layoutId", layoutId);
    }

    @Override
    public EnrichmentLayout findBySourceId(String customerSpace, String sourceId) {
        return enrichmentLayoutEntityMgr.findByField("sourceId", sourceId);
    }

    @Override
    public void deleteLayoutByLayoutId(String customerSpace, String layoutId) {
        EnrichmentLayout enrichmentLayout = findByLayoutId(customerSpace, layoutId);
        if (null != enrichmentLayout) {
            deleteLayout(enrichmentLayout);
        }
    }

    @Override
    public void deleteLayout(EnrichmentLayout enrichmentLayout) {
        enrichmentLayout.setDeleted(Boolean.TRUE);
        enrichmentLayoutEntityMgr.update(enrichmentLayout);
    }

    @Override
    public void hardDeleteLayout(EnrichmentLayout enrichmentLayout) {
        enrichmentLayoutEntityMgr.delete(enrichmentLayout);
    }

    @Override
    public void hardDeleteLayoutByLayoutId(String customerSpace, String layoutId) {
        EnrichmentLayout enrichmentLayout = findByLayoutId(customerSpace, layoutId);
        if (null != enrichmentLayout) {
            hardDeleteLayout(enrichmentLayout);
        }
    }

    /**
     * Validate the enrichment layout.
     *
     * From DCP-1629: validation fails if subscriber is not entitled to given
     * elements for given domain and record type. Error should include specific
     * elements that are cause validation to fail
     * 
     * @param enrichmentLayout
     *            - The object to validate
     * @return an EnrichmentLayoutValidationResult that tells if the layout is valid
     *         and why it isn't.
     */
    protected EnrichmentLayoutOperationResult validate(EnrichmentLayout enrichmentLayout) {
        // Are required fields present?
        EnrichmentLayoutOperationResult result;
        if (enrichmentLayout.getSourceId() == null || enrichmentLayout.getDomain() == null
                || enrichmentLayout.getRecordType() == null || enrichmentLayout.getTenant() == null) {
            StringBuilder builder = new StringBuilder();
            if (enrichmentLayout.getSourceId() == null) {
                builder.append("Required field SourceId is null\n");
            }
            if (enrichmentLayout.getDomain() == null) {
                builder.append("Required field Domain is null\n");
            }
            if (enrichmentLayout.getRecordType() == null) {
                builder.append("Required field RecordType is null\n");
            }
            if (enrichmentLayout.getTenant() == null) {
                builder.append("Required field Tenant is null\n");
            }
            result = new EnrichmentLayoutOperationResult(false, builder.toString());
        } else {
            String tenantId = enrichmentLayout.getTenant().getId();
            DataBlockEntitlementContainer dataBlockEntitlementContainer = appendConfigService.getEntitlement(tenantId);
            result = validateDomain(enrichmentLayout, dataBlockEntitlementContainer);
        }
        return result;
    }

    private EnrichmentLayoutOperationResult validateDataRecordType(EnrichmentLayout enrichmentLayout,
            Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> map) {

        // Get a Set of the blockId and level values that the tenant must have for the
        // layout to be valid
        Set<String> blocksContainingElements = primeMetadataProxy
                .getBlocksContainingElements(enrichmentLayout.getElements());

        // Get a list of the datablocks available for the dataRecordType in this tenant.
        // This is used to determine if the required blocks are present.
        DataRecordType dataRecordType = enrichmentLayout.getRecordType();
        List<DataBlockEntitlementContainer.Block> dataBlockList = map.get(dataRecordType);
        if (dataBlockList != null) { // build a set of authorized data blocks and levels
            Set<String> authorizedElements = new HashSet<>();
            for (DataBlockEntitlementContainer.Block block : dataBlockList) {
                for (DataBlockLevel level : block.getDataBlockLevels()) {
                    String element = block.getBlockId() + "_" + level.name();
                    authorizedElements.add(element);
                }
            }

            // Now iterate through the blocks and levels that the layout needs and make sure
            // they are avail for the tenant
            for (String neededElement : blocksContainingElements) {
                String checkingString = neededElement.substring(0, neededElement.lastIndexOf("_")); // trim the version
                                                                                                    // value off the end
                if (!authorizedElements.contains(checkingString)) { // if not in Set then the layout isn't valid
                    return new EnrichmentLayoutOperationResult(false, String.format(
                            "EnrichmentLayout is not valid, element %s is not authorized for subscriber number %s.",
                            neededElement, enrichmentLayout.getTenant().getSubscriberNumber()));
                }
            }
            return new EnrichmentLayoutOperationResult(true, "EnrighmentLayout is valid.");
        } else {
            return new EnrichmentLayoutOperationResult(false,
                    String.format("Data Record Type %s does not contain any data blocks.", dataRecordType.name()));

        }
    }

    private EnrichmentLayoutOperationResult validateDomain(EnrichmentLayout enrichmentLayout,
            DataBlockEntitlementContainer dataBlockEntitlementContainer) {
        // Get the list of domains
        List<DataBlockEntitlementContainer.Domain> entitledDomains = dataBlockEntitlementContainer.getDomains();

        // Check that the EnrichmentLayout domain is in the list of domains for this
        // tenant
        for (DataBlockEntitlementContainer.Domain domain : entitledDomains) {
            if (domain.getDomain() == enrichmentLayout.getDomain()) {
                return validateDataRecordType(enrichmentLayout, domain.getRecordTypes()); // If it is then check the
                                                                                          // record type
            }
        }
        return new EnrichmentLayoutOperationResult(false,
                String.format("Enrichment Layout is not valid. %s domain is not valid domain for this user.",
                        enrichmentLayout.getDomain().name()));
    }

}
