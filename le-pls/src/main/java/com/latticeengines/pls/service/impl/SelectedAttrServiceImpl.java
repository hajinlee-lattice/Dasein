package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DatabaseUtils;
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
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.pls.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.pls.service.SelectedAttrService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("selectedAttrService")
public class SelectedAttrServiceImpl implements SelectedAttrService {
    private static final String SPACE_DELIM = " ";

    private static final Log log = LogFactory.getLog(SelectedAttrServiceImpl.class);

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
    private TenantConfigServiceImpl tenantConfigService;

    @Override
    public void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant,
            Map<String, Integer> limitationMap) {
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        Integer premiumAttributeLimitation = getTotalMaxCount(limitationMap);
        checkAmbiguityInFieldNames(attributes);

        int existingSelectedAttributePremiumCount = getSelectedAttributePremiumCount(tenant);
        int additionalPremiumAttrCount = 0;
        List<LeadEnrichmentAttribute> allAttributes = getAttributes(tenant, null, null, null, false, null, null);

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
                LeadEnrichmentAttribute deselectedAttr = findEnrichmentAttributeByName(allAttributes,
                        deselectedAttrStr);
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
            Category category, String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max) {
        List<SelectedAttribute> selectedAttributes = selectedAttrEntityMgr.findAll();

        List<String> selectedAttributeInternalNames = getselectedAttributeInternalNames(selectedAttributes);

        // TODO - pass selectedAttributeInternalNames to columnSelection() to
        // get metadata about only these names
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(Predefined.Enrichment, //
                MatchTypeUtil.getVersionForEnforcingAccountMasterBasedMatch());

        return superimpose(allColumns, selectedAttributes, attributeDisplayNameFilter, category, subcategory,
                onlySelectedAttributes, offset, max);
    }

    @Override
    public int getAttributesCount(Tenant tenant, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes) {
        // TODO - update this to use efficient way to get count
        List<LeadEnrichmentAttribute> matchingAttr = getAttributes(tenant, attributeDisplayNameFilter, category,
                subcategory, onlySelectedAttributes, null, null);
        return (matchingAttr == null) ? 0 : matchingAttr.size();
    }

    private List<String> getselectedAttributeInternalNames(List<SelectedAttribute> selectedAttributes) {
        List<String> selectedAttributeInternalNames = new ArrayList<>();
        for (SelectedAttribute attr : selectedAttributes) {
            selectedAttributeInternalNames.add(attr.getColumnId());
        }
        return selectedAttributeInternalNames;
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
        int premiumAttributesLimitation = tenantConfigService.getMaxPremiumLeadEnrichmentAttributes(tenant.getId());
        limitationMap.put("HGData_Pivoted_Source", premiumAttributesLimitation);
        return limitationMap;
    }

    @Override
    public void downloadAttributes(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName, Tenant tenant, Boolean isSelected) {
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(Predefined.Enrichment, //
                MatchTypeUtil.getVersionForEnforcingAccountMasterBasedMatch());
        List<SelectedAttribute> selectedAttributes = selectedAttrEntityMgr.findAll();

        List<LeadEnrichmentAttribute> attributes = superimpose(allColumns, selectedAttributes, null, null, null,
                isSelected, null, null);
        DlFileHttpDownloader downloader = new DlFileHttpDownloader(mimeType, fileName,
                getCSVFromAttributes(attributes));
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
            stringBuffer.append(modifyStringForCSV(getDataTypeDisplay(attribute.getFieldType())) + ",");
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
        case "NVARCHAR(3)":
            displayName = "Text (3)";
            break;
        default:
            displayName = "Text (3)";
            break;
        }

        return displayName;
    }

    private List<LeadEnrichmentAttribute> superimpose(List<ColumnMetadata> allColumns,
            List<SelectedAttribute> selectedAttributes, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max) {
        if ((offset != null && offset < 0) || (max != null && max <= 0)) {
            // TODO - throw LEDP exception
            throw new RuntimeException("Invalid pagination option");
        }

        List<String> selectedAttributeNames = new ArrayList<>();

        for (SelectedAttribute selectedAttribute : selectedAttributes) {
            selectedAttributeNames.add(selectedAttribute.getColumnId());
        }

        List<LeadEnrichmentAttribute> superimposedList = new ArrayList<>();
        List<String> searchTokens = getSearchTokens(attributeDisplayNameFilter);

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

                if (subcategory != null //
                        && (column.getSubcategory() == null //
                                || (column.getSubcategory() != null //
                                        && !subcategory.equals(column.getSubcategory())))//
                ) {
                    continue;
                }
            }

            if (!isMatchingSearchTokens(searchTokens, column)) {
                continue;
            }

            addAttrInFinalList(selectedAttributeNames, superimposedList, column);
        }

        return extractPage(superimposedList, offset, max);
    }

    private List<String> getSearchTokens(String attributeDisplayNameFilter) {
        Set<String> searchTokens = new HashSet<>();

        // tokenize and find set of unique tokens from search string
        if (!StringUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)) {
            StringTokenizer st = new StringTokenizer(attributeDisplayNameFilter.trim(), SPACE_DELIM);
            while (st.hasMoreTokens()) {
                searchTokens.add(st.nextToken().toUpperCase());
            }
        }

        return new ArrayList<String>(searchTokens);
    }

    private boolean isMatchingSearchTokens(List<String> searchTokens, ColumnMetadata column) {
        // if column's display name does not contain any of the search tokens
        // then return false otherwise retrun true
        //
        // note that good searching result comes if order of search tokens is
        // not important. Otherwise search becomes very restrictive

        if (!CollectionUtils.isEmpty(searchTokens)) {
            String displayName = column.getDisplayName().toUpperCase();
            for (String token : searchTokens) {
                if (!displayName.contains(token)) {
                    return false;
                }
            }
        }
        return true;
    }

    private List<LeadEnrichmentAttribute> extractPage(List<LeadEnrichmentAttribute> superimposedList, Integer offset,
            Integer max) {
        if (offset == null && max == null) {
            return superimposedList;
        } else {
            if (offset != null && offset >= superimposedList.size()) {
                return new ArrayList<LeadEnrichmentAttribute>();
            }

            int effectiveStartIndex = offset == null ? 0 : offset;
            int effectiveEndIndex = max == null ? superimposedList.size() : effectiveStartIndex + max;
            if (effectiveEndIndex > superimposedList.size()) {
                effectiveEndIndex = superimposedList.size();
            }

            return superimposedList.subList(effectiveStartIndex, effectiveEndIndex);
        }
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
        attr.setSubcategory(column.getSubcategory() == null ? //
                null : column.getSubcategory().toString());
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

    private Integer getTotalMaxCount(Map<String, Integer> limitationMap) {
        Integer totalCount = 0;
        for (String key : limitationMap.keySet()) {
            totalCount += limitationMap.get(key);
        }
        return totalCount;
    }
}
