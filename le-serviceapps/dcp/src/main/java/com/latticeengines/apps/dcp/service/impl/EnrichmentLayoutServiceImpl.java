package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
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

    private static final String RANDOM_ENRICHMENT_LAYOUT_ID_PATTERN = "Layout_%s";

    @Override
    public ResponseDocument<String> create(String customerSpace, EnrichmentLayout enrichmentLayout) {
        if (null == enrichmentLayout.getLayoutId()) {
            enrichmentLayout.setLayoutId(createLayoutId());
        }
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        enrichmentLayout.setTenant(tenant);
        ResponseDocument<String> result = validate(enrichmentLayout);
        if (result.isSuccess()) {
            enrichmentLayoutEntityMgr.create(enrichmentLayout);
            result.setResult(enrichmentLayout.getLayoutId());
        }
        return result;
    }

    @Override
    public List<EnrichmentLayoutDetail> getAll(String customerSpace, int pageIndex, int pageSize) {
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        return enrichmentLayoutEntityMgr.findAllEnrichmentLayoutDetail(pageRequest);
    }

    @Override
    public ResponseDocument<String> update(String customerSpace, EnrichmentLayout enrichmentLayout) {
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        enrichmentLayout.setTenant(tenant);
        ResponseDocument<String> result = validate(enrichmentLayout);
        if (result.isSuccess()) {
            enrichmentLayoutEntityMgr.update(enrichmentLayout);
            result.setResult(enrichmentLayout.getLayoutId());
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
    public void deleteLayout(EnrichmentLayout enrichmentLayout) {
        enrichmentLayoutEntityMgr.delete(enrichmentLayout);
    }

    @Override
    public void deleteLayoutByLayoutId(String customerSpace, String layoutId) {
        EnrichmentLayout enrichmentLayout = findByLayoutId(customerSpace, layoutId);
        if (null != enrichmentLayout) {
            deleteLayout(enrichmentLayout);
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
    protected ResponseDocument<String> validate(EnrichmentLayout enrichmentLayout) {
        // Are required fields present?
        ResponseDocument<String> result;
        if (enrichmentLayout.getSourceId() == null || enrichmentLayout.getDomain() == null
                || enrichmentLayout.getRecordType() == null || enrichmentLayout.getTenant() == null) {
            List<String> errors = new ArrayList<>();
            if (enrichmentLayout.getSourceId() == null) {
                errors.add("Required field SourceId is null");
            }
            if (enrichmentLayout.getDomain() == null) {
                errors.add("Required field Domain is null");
            }
            if (enrichmentLayout.getRecordType() == null) {
                errors.add("Required field RecordType is null");
            }
            if (enrichmentLayout.getTenant() == null) {
                errors.add("Required field Tenant is null");
            }
            result = new ResponseDocument<>();
            result.setErrors(errors);
        } else {
            // does this source already have an enrichmentlayout?
            String sourceId = enrichmentLayout.getSourceId();
            EnrichmentLayoutDetail el = enrichmentLayoutEntityMgr.findEnrichmentLayoutDetailBySourceId(sourceId);
            if (null != el) {
                result = new ResponseDocument<>();
                result.setErrors(Collections.singletonList( //
                        String.format("Can't create.  SourceId %s already has an EnrichmentLayout and each sourceId can only have one.", //
                                sourceId)));
            }
            else {
                String tenantId = enrichmentLayout.getTenant().getId();
                DataBlockEntitlementContainer dataBlockEntitlementContainer = appendConfigService.getEntitlement(tenantId);
                result = validateDomain(enrichmentLayout, dataBlockEntitlementContainer);
            }
        }
        return result;
    }

    private ResponseDocument<String> validateDataRecordType(EnrichmentLayout enrichmentLayout,
            Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> map) {

        List<String> errors = new ArrayList<>();

        // Get a Set of the blockId and level values that the tenant must have for the
        // layout to be valid.
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
                    String err = String.format(
                            "EnrichmentLayout is not valid, element %s is not authorized for subscriber number %s.",
                            neededElement, enrichmentLayout.getTenant().getSubscriberNumber());
                    errors.add(err);
                    /*
                     * return new EnrichmentLayoutOperationResult(false, String.format(
                     * "EnrichmentLayout is not valid, element %s is not authorized for subscriber number %s."
                     * , neededElement, enrichmentLayout.getTenant().getSubscriberNumber()));
                     */
                }
            }
        } else {
            errors.add(String.format("Data Record Type %s does not contain any data blocks.", dataRecordType.name()));
            /*
             * return new EnrichmentLayoutOperationResult(false,
             * String.format("Data Record Type %s does not contain any data blocks.",
             * dataRecordType.name()));
             */

        }
        if (errors.isEmpty()) {
            return ResponseDocument.successResponse("");
        } else {
            ResponseDocument<String> rd0 = new ResponseDocument<>();
            rd0.setErrors(errors);
            return rd0;
        }
    }

    private ResponseDocument<String> validateDomain(EnrichmentLayout enrichmentLayout,
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
        ResponseDocument<String> response = new ResponseDocument<>();
        response.setErrors(Collections.singletonList(
                String.format("Enrichment Layout is not valid. %s domain is not valid domain for this user.",
                        enrichmentLayout.getDomain().name())));
        return response;
    }

    private String createLayoutId() {
        String randomLayoutId = String.format(RANDOM_ENRICHMENT_LAYOUT_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (enrichmentLayoutEntityMgr.findEnrichmentLayoutDetailByLayoutId(randomLayoutId) != null) {
            randomLayoutId = String.format(RANDOM_ENRICHMENT_LAYOUT_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomLayoutId;
    }

}
