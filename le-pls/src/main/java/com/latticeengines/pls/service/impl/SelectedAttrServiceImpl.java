package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.StringUtils;
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

        checkAmbiguityInFieldNames(attributes);

        int existingSelectedAttributePremiumCount = getSelectedAttributePremiumCount(tenant);
        int additionalPremiumAttrCount = 0;
        List<LeadEnrichmentAttribute> allAttributes = getAttributes(tenant, null, null, false);

        List<SelectedAttribute> addAttrList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes.getSelectedAttributes())) {
            for (String selectedAttrStr : attributes.getSelectedAttributes()) {
                LeadEnrichmentAttribute selectedAttr = findEnrichmentAttributeByName(allAttributes, selectedAttrStr);
                SelectedAttribute attr = populateAttrObj(selectedAttr, tenant);
                addAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount++;
                }
            }
        }

        List<SelectedAttribute> deleteAttrList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributes.getDeselectedAttributes())) {
            for (String deselectedAttrStr : attributes.getDeselectedAttributes()) {
                LeadEnrichmentAttribute deselectedAttr = findEnrichmentAttributeByName(allAttributes,
                        deselectedAttrStr);
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

    @Override
    public List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter,
            Category category, Boolean onlySelectedAttributes) {
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(Predefined.LeadEnrichment);
        List<SelectedAttribute> selectedAttributes = selectedAttrEntityMgr.findAll();

        return superimpose(allColumns, selectedAttributes, attributeDisplayNameFilter, category,
                onlySelectedAttributes);
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
    public Map<String, Integer> getPremiumAttributesLimitation(Tenant tenant) {
        Map<String, Integer> limitationMap = new HashMap<>();
        limitationMap.put("HGData_Pivoted_Source", premiumAttributesLimitation);
        return limitationMap;
    }

    private List<LeadEnrichmentAttribute> superimpose(List<ColumnMetadata> allColumns,
            List<SelectedAttribute> selectedAttributes, String attributeDisplayNameFilter, Category category,
            Boolean onlySelectedAttributes) {
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

            if (category != null) {
                if (column.getCategory() != category) {
                    continue;
                }
            }

            if (!StringUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)) {
                if (!column.getDisplayName().toUpperCase().contains(attributeDisplayNameFilter.toUpperCase())) {
                    continue;
                }
            }

            addAttrInFinalList(selectedAttributeNames, superimposedList, column);
        }

        return superimposedList;
    }

    private void addAttrInFinalList(List<String> selectedAttributeNames, List<LeadEnrichmentAttribute> superimposedList,
            ColumnMetadata column) {
        LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
        attr.setDisplayName(column.getDisplayName());
        attr.setFieldName(column.getColumnId());
        attr.setFieldNameInTarget(column.getColumnName());
        attr.setFieldType(column.getDataType());
        attr.setDataSource(column.getMatchDestination());
        attr.setDescription(column.getDescription());
        attr.setIsSelected(selectedAttributeNames.contains(column.getColumnId()));
        attr.setIsPremium(column.isPremium());
        attr.setCategory(column.getCategory().toString());
        superimposedList.add(attr);
    }

    private void checkAmbiguityInFieldNames(LeadEnrichmentAttributesOperationMap attributes) {
        Set<String> attrSet = new HashSet<>();
        checkDuplicate(attrSet, attributes.getSelectedAttributes());
        checkDuplicate(attrSet, attributes.getDeselectedAttributes());
    }

    private void checkDuplicate(Set<String> attrSet, List<String> attributes) {
        if (!CollectionUtils.isEmpty(attributes)) {
            for (String attrStr : attributes) {
                if (attrSet.contains(attrStr)) {
                    throw new LedpException(LedpCode.LEDP_18113, new String[] { attrStr });
                }
                attrSet.add(attrStr);
            }
        }
    }

    private LeadEnrichmentAttribute findEnrichmentAttributeByName(List<LeadEnrichmentAttribute> allAttributes,
            String selectedAttrStr) {
        for (LeadEnrichmentAttribute attr : allAttributes) {
            if (attr.getFieldName().equals(selectedAttrStr)) {
                return attr;
            }
        }

        throw new LedpException(LedpCode.LEDP_18114, new String[] { selectedAttrStr });
    }

    private SelectedAttribute populateAttrObj(LeadEnrichmentAttribute leadEnrichmentAttr, Tenant tenant) {
        SelectedAttribute attr = new SelectedAttribute(leadEnrichmentAttr.getFieldName(), tenant);
        attr.setIsPremium(leadEnrichmentAttr.getIsPremium());
        return attr;
    }
}
