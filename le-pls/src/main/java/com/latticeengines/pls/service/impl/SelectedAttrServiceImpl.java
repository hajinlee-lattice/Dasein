package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.pls.service.SelectedAttrService;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("selectedAttrService")
public class SelectedAttrServiceImpl implements SelectedAttrService {
    @Autowired
    private SelectedAttrEntityMgr selectedAttrEntityMgr;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${pls.leadenrichment.premium.max:10}")
    private int premiumAttributesLimitation;

    @Override
    public void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant,
            Integer premiumAttributeLimitation) {
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        int existingSelectedAttributePremiumCount = getSelectedAttributePremiumCount(tenant);
        int additionalPremiumAttrCount = 0;

        List<SelectedAttribute> addAttrList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes.getSelectedAttributes())) {
            for (LeadEnrichmentAttribute selectedAttr : attributes.getSelectedAttributes()) {
                SelectedAttribute attr = populateAttrObj(selectedAttr, tenant);
                addAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount++;
                }
            }
        }

        List<SelectedAttribute> deleteAttrList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributes.getDeselectedAttributes())) {
            for (LeadEnrichmentAttribute deselectedAttr : attributes.getDeselectedAttributes()) {
                SelectedAttribute attr = populateAttrObj(deselectedAttr, tenant);
                deleteAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount--;
                }
            }
        }

        if (premiumAttributeLimitation < existingSelectedAttributePremiumCount + additionalPremiumAttrCount) {
            // throw exception if effective premium count crosses limitation
            throw new LedpException(LedpCode.LEDP_18112, new String[] { premiumAttributeLimitation.toString() });
        }

        selectedAttrEntityMgr.add(addAttrList);
        selectedAttrEntityMgr.delete(deleteAttrList);
    }

    private SelectedAttribute populateAttrObj(LeadEnrichmentAttribute leadEnrichmentAttr, Tenant tenant) {
        SelectedAttribute attr = new SelectedAttribute(leadEnrichmentAttr.getFieldName(), tenant);
        attr.setIsPremium(leadEnrichmentAttr.getIsPremium());
        return attr;
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter,
            Category category, Boolean onlySelectedAttributes) {
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(Predefined.LeadEnrichment);
        List<SelectedAttribute> selectedAttributes = selectedAttrEntityMgr.findAll();

        return superimpose(allColumns, selectedAttributes, onlySelectedAttributes);
    }

    private List<LeadEnrichmentAttribute> superimpose(List<ColumnMetadata> allColumns,
            List<SelectedAttribute> selectedAttributes, Boolean onlySelectedAttributes) {
        List<String> selectedAttributeNames = new ArrayList<>();

        for (SelectedAttribute selectedAttribute : selectedAttributes) {
            selectedAttributeNames.add(selectedAttribute.getColumnId());
        }

        List<LeadEnrichmentAttribute> superimposedList = new ArrayList<>();

        for (ColumnMetadata column : allColumns) {
            if (onlySelectedAttributes == Boolean.TRUE) {
                if (!selectedAttributeNames.contains(column.getColumnId())) {
                    continue;
                }
            }
            LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
            attr.setDisplayName(column.getDisplayName());
            attr.setFieldName(column.getColumnId());
            attr.setFieldNameInTarget(column.getColumnName());
            attr.setFieldType(column.getDataType());
            attr.setDataSource(column.getMatchDestination());
            attr.setDescription(column.getDescription());
            attr.setIsSelected(selectedAttributeNames.contains(column.getColumnId()));
            attr.setIsPremium(column.isPremium());
            attr.setCategory(column.getCategory());
            superimposedList.add(attr);
        }

        return superimposedList;
    }

    @Override
    public Integer getSelectedAttributeCount(Tenant tenant) {
        return selectedAttrEntityMgr.count(false);
    }

    @Override
    public Integer getSelectedAttributePremiumCount(Tenant tenant) {
        return selectedAttrEntityMgr.count(true);
    }

    @Override
    public Integer getPremiumAttributesLimitation(Tenant tenant) {
        return premiumAttributesLimitation;
    }

}
