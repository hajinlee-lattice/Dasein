package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.download.DlFileHttpDownloader;
import com.latticeengines.app.exposed.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.app.exposed.service.SelectedAttrService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("selectedAttrService")
public class SelectedAttrServiceImpl implements SelectedAttrService {

    private static final String UNIQUE_CONSTRAINT_SELECTED_ATTRIBUTES = "UQ__SELECTED__";

    private static final int MAX_SAVE_RETRY = 5;

    private static List<String> CSV_HEADERS = Arrays.asList("Attribute", "Category", "Description", "Data Type",
            "Status", "Premium");

    @Autowired
    private SelectedAttrEntityMgr selectedAttrEntityMgr;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private AppTenantConfigServiceImpl appTenantConfigService;

    @Override
    public void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant,
            Map<String, Integer> limitationMap, Boolean considerInternalAttributes) {
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        Integer premiumAttributeLimitation = getTotalMaxCount(limitationMap);
        checkAmbiguityInFieldNames(attributes);

        int existingSelectedAttributePremiumCount = getSelectedAttributePremiumCount(tenant, considerInternalAttributes);
        int additionalPremiumAttrCount = 0;
        List<LeadEnrichmentAttribute> allAttributes = getAttributes(tenant, null, null, null, false, null, null,
                considerInternalAttributes);

        final List<SelectedAttribute> addAttrList = new ArrayList<>();
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

        final List<SelectedAttribute> deleteAttrList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributes.getDeselectedAttributes())) {
            for (String deselectedAttrStr : attributes.getDeselectedAttributes()) {
                LeadEnrichmentAttribute deselectedAttr = findEnrichmentAttributeByName(allAttributes, deselectedAttrStr);
                SelectedAttribute attr = populateAttrObj(deselectedAttr, tenant);
                deleteAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount--;
                }
            }
        }

        // TODO - add check for per datasource limitation as well
        if (premiumAttributeLimitation < existingSelectedAttributePremiumCount + additionalPremiumAttrCount) {
            // throw exception if effective premium count crosses limitation
            throw new LedpException(LedpCode.LEDP_18112, new String[] { premiumAttributeLimitation.toString() });
        }

        DatabaseUtils.retry("saveEnrichmentAttributeSelection", MAX_SAVE_RETRY, ConstraintViolationException.class,
                "Ignoring ConstraintViolationException exception and retrying save operation",
                UNIQUE_CONSTRAINT_SELECTED_ATTRIBUTES, new Closure() {
                    @Override
                    public void execute(Object input) {
                        selectedAttrEntityMgr.upsert(addAttrList, deleteAttrList);
                    }
                });
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter,
            Category category, String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
            Boolean considerInternalAttributes) {
        AttributePageProcessor processor = new AttributePageProcessor.Builder()
                .columnMetadataProxy(columnMetadataProxy) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .attributeDisplayNameFilter(attributeDisplayNameFilter) //
                .category(category) //
                .subcategory(subcategory) //
                .onlySelectedAttributes(onlySelectedAttributes) //
                .offset(offset) //
                .max(max) //
                .considerInternalAttributes(considerInternalAttributes) //
                .build();

        return processor.getPage();
    }

    @Override
    public List<LeadEnrichmentAttribute> getAllAttributes() {
        AttributePageProcessor processor = new AttributePageProcessor.Builder()
                .columnMetadataProxy(columnMetadataProxy) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .considerInternalAttributes(true).build();
        return processor.getPage();
    }

    @Override
    public int getAttributesCount(Tenant tenant, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        List<LeadEnrichmentAttribute> matchingAttr = getAttributes(tenant, attributeDisplayNameFilter, category,
                subcategory, onlySelectedAttributes, null, null, considerInternalAttributes);
        return (matchingAttr == null) ? 0 : matchingAttr.size();
    }

    @Override
    public Integer getSelectedAttributeCount(Tenant tenant, Boolean considerInternalAttributes) {
        return getSelectedAttrCount(tenant, Boolean.FALSE, considerInternalAttributes);
    }

    @Override
    public Integer getSelectedAttributePremiumCount(Tenant tenant, Boolean considerInternalAttributes) {
        return getSelectedAttrCount(tenant, Boolean.TRUE, considerInternalAttributes);
    }

    protected Integer getSelectedAttrCount(Tenant tenant, Boolean countOnlySelectedPremiumAttr,
            Boolean considerInternalAttributes) {
        List<LeadEnrichmentAttribute> attributeList = getAttributes(tenant, null, null, null, null, null, null,
                considerInternalAttributes);
        int count = 0;
        for (LeadEnrichmentAttribute attr : attributeList) {
            if (attr.getIsSelected()) {
                if (countOnlySelectedPremiumAttr == Boolean.TRUE) {
                    if (attr.getIsPremium()) {
                        count++;
                    }
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Map<String, Integer> getPremiumAttributesLimitation(Tenant tenant) {
        Map<String, Integer> limitationMap = new HashMap<>();
        int premiumAttributesLimitation = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributes(tenant.getId());
        limitationMap.put("HGData_Pivoted_Source", premiumAttributesLimitation);
        return limitationMap;
    }

    @Override
    public void downloadAttributes(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName, Tenant tenant, Boolean isSelected, Boolean considerInternalAttributes) {
        AttributePageProcessor processor = new AttributePageProcessor.Builder()
                .columnMetadataProxy(columnMetadataProxy) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .onlySelectedAttributes(isSelected) //
                .considerInternalAttributes(considerInternalAttributes) //
                .build();

        List<LeadEnrichmentAttribute> attributes = processor.getPage();
        DlFileHttpDownloader downloader = new DlFileHttpDownloader(mimeType, fileName, getCSVFromAttributes(attributes));
        downloader.downloadFile(request, response);
    }

    private String getCSVFromAttributes(List<LeadEnrichmentAttribute> attributes) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < CSV_HEADERS.size(); i++) {
            if (i + 1 < CSV_HEADERS.size()) {
                stringBuffer.append(modifyStringForCSV(CSV_HEADERS.get(i)) + ",");
            } else {
                stringBuffer.append(modifyStringForCSV(CSV_HEADERS.get(i)));
            }
        }
        stringBuffer.append("\r\n");

        for (LeadEnrichmentAttribute attribute : attributes) {
            stringBuffer.append(modifyStringForCSV(attribute.getDisplayName()) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getCategory()) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getDescription()) + ",");
            stringBuffer.append(modifyStringForCSV(getDataTypeDisplay(attribute.getFieldJavaType())) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getIsSelected() ? "On" : "Off") + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getIsPremium() ? "Yes" : "No"));
            stringBuffer.append("\r\n");
        }

        return stringBuffer.toString();
    }

    private String modifyStringForCSV(String value) {
        if (value == null) {
            return "";
        }
        return "\"" + value.replaceAll("\"", "\"\"") + "\"";
    }

    private String getDataTypeDisplay(String dataType) {
        String displayName;

        switch (dataType.toUpperCase()) {
        case "BOOLEAN":
            displayName = "Boolean";
            break;
        case "DOUBLE":
        case "INTEGER":
        case "LONG":
            displayName = "Number";
            break;
        case "STRING":
        default:
            displayName = "Text";
            break;
        }

        return displayName;
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

    private Integer getTotalMaxCount(Map<String, Integer> limitationMap) {
        Integer totalCount = 0;
        for (String key : limitationMap.keySet()) {
            totalCount += limitationMap.get(key);
        }
        return totalCount;
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

}
