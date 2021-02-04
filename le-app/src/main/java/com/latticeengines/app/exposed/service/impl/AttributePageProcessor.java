package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.app.exposed.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.util.ImportanceOrderingUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class AttributePageProcessor {
    private static final String SPACE_DELIM = " ";

    private final ColumnMetadataProxy columnMetadataProxy;
    private BatonService batonService;
    private final SelectedAttrEntityMgr selectedAttrEntityMgr;
    private final String attributeDisplayNameFilter;
    private final Category category;
    private final String subcategory;
    private final Boolean onlySelectedAttributes;
    private final Integer offset;
    private final Integer max;
    private final Boolean considerInternalAttributes;
    private final AttributeCustomizationService attributeCustomizationService;
    private final boolean skipTenantLevelCustomization;


    public static class Builder {
        private SelectedAttrEntityMgr selectedAttrEntityMgr;
        private ColumnMetadataProxy columnMetadataProxy;
        private BatonService batonService;
        private String attributeDisplayNameFilter;
        private Category category;
        private String subcategory;
        private Boolean onlySelectedAttributes;
        private Integer offset;
        private Integer max;
        private Boolean considerInternalAttributes;
        private AttributeCustomizationService attributeCustomizationService;
        private boolean skipTenantLevelCustomization;

        public Builder selectedAttrEntityMgr(SelectedAttrEntityMgr selectedAttrEntityMgr) {
            this.selectedAttrEntityMgr = selectedAttrEntityMgr;
            return this;
        }

        public Builder columnMetadataProxy(ColumnMetadataProxy columnMetadataProxy) {
            this.columnMetadataProxy = columnMetadataProxy;
            return this;
        }

        public Builder batonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }

        public Builder attributeDisplayNameFilter(String attributeDisplayNameFilter) {
            this.attributeDisplayNameFilter = attributeDisplayNameFilter;
            return this;
        }

        public Builder category(Category category) {
            this.category = category;
            return this;
        }

        public Builder subcategory(String subcategory) {
            this.subcategory = subcategory;
            return this;
        }

        public Builder onlySelectedAttributes(Boolean onlySelectedAttributes) {
            this.onlySelectedAttributes = onlySelectedAttributes;
            return this;
        }

        public Builder offset(Integer offset) {
            this.offset = offset;
            return this;
        }

        public Builder max(Integer max) {
            this.max = max;
            return this;
        }

        public Builder considerInternalAttributes(Boolean considerInternalAttributes) {
            this.considerInternalAttributes = considerInternalAttributes;
            return this;
        }

        public Builder skipTenantLevelCustomization(boolean skipTenantLevelCustomization) {
            this.skipTenantLevelCustomization = skipTenantLevelCustomization;
            return this;
        }

        public AttributePageProcessor build() {
            return new AttributePageProcessor(columnMetadataProxy, batonService, selectedAttrEntityMgr,
                    attributeCustomizationService, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, offset, max, considerInternalAttributes, skipTenantLevelCustomization);
        }

        public Builder attributeCustomizationService(AttributeCustomizationService attributeCustomizationService) {
            this.attributeCustomizationService = attributeCustomizationService;
            return this;
        }
    }

    private AttributePageProcessor(ColumnMetadataProxy columnMetadataProxy, BatonService batonService,
            SelectedAttrEntityMgr selectedAttrEntityMgr, AttributeCustomizationService attributeCustomizationService,
            String attributeDisplayNameFilter, Category category, String subcategory, Boolean onlySelectedAttributes,
            Integer offset, Integer max, Boolean considerInternalAttributes, boolean skipTenantLevelCustomization) {
        this.columnMetadataProxy = columnMetadataProxy;
        this.batonService = batonService;
        this.selectedAttrEntityMgr = selectedAttrEntityMgr;
        this.attributeCustomizationService = attributeCustomizationService;
        this.attributeDisplayNameFilter = attributeDisplayNameFilter;
        this.category = category;
        this.subcategory = subcategory;
        this.onlySelectedAttributes = onlySelectedAttributes;
        this.offset = offset;
        this.max = max;
        this.considerInternalAttributes = considerInternalAttributes;
        this.skipTenantLevelCustomization = skipTenantLevelCustomization;
    }

    public List<LeadEnrichmentAttribute> getPage() {
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Enrichment);
        String tenantId = MultiTenantContext.getShortTenantId();
        Set<String> expiredLicenses = batonService.getExpiredLicenses(tenantId);
        allColumns = allColumns.stream().filter(cm -> !expiredLicenses.contains(cm.getDataLicense()))
                .collect(Collectors.toList());
        List<SelectedAttribute> selectedAttributes = skipTenantLevelCustomization ? new ArrayList<>()
                : selectedAttrEntityMgr.findAll();
        List<LeadEnrichmentAttribute> attributes = superimpose(allColumns, selectedAttributes,
                attributeDisplayNameFilter, category, subcategory, onlySelectedAttributes, offset, max,
                considerInternalAttributes);
        if (!skipTenantLevelCustomization) {
            attributeCustomizationService.addFlags(attributes.stream().map(c -> (HasAttributeCustomizations) c)
                    .collect(Collectors.toList()));
        }
        ImportanceOrderingUtils.addImportanceOrderingToLeadEnrichmentAttrs(attributes);
        return attributes;
    }

    private List<LeadEnrichmentAttribute> superimpose(List<ColumnMetadata> allColumns,
            List<SelectedAttribute> selectedAttributes, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
            Boolean considerInternalAttributes) {
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
            if (onlySelectedAttributes == Boolean.TRUE //
                    && !selectedAttributeNames.contains(column.getAttrName())) {
                continue;
            }

            if (considerInternalAttributes != Boolean.TRUE //
                    && Boolean.TRUE.equals(column.getCanInternalEnrich())) {
                continue;
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

            superimposedList.add(constructAttribute(selectedAttributeNames, column));
        }

        return extractPage(superimposedList, offset, max);
    }

    private List<String> getSearchTokens(String attributeDisplayNameFilter) {
        Set<String> searchTokens = new HashSet<>();

        // tokenize and find set of unique tokens from search string
        if (!StringStandardizationUtils.objectIsNullOrEmptyString(attributeDisplayNameFilter)) {
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
                return new ArrayList<>();
            }

            int effectiveStartIndex = offset == null ? 0 : offset;
            int effectiveEndIndex = max == null ? superimposedList.size() : effectiveStartIndex + max;
            if (effectiveEndIndex > superimposedList.size()) {
                effectiveEndIndex = superimposedList.size();
            }

            return superimposedList.subList(effectiveStartIndex, effectiveEndIndex);
        }
    }

    @SuppressWarnings("deprecation")
    private LeadEnrichmentAttribute constructAttribute(List<String> selectedAttributeNames, ColumnMetadata column) {
        LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
        attr.setDisplayName(column.getDisplayName());
        attr.setFieldName(column.getAttrName());
        attr.setFieldNameInTarget(column.getAttrName());
        attr.setColumnId(column.getAttrName());
        attr.setJavaClass(column.getJavaClass());
        attr.setFieldType(column.getDataType());
        attr.setFieldJavaType(column.getJavaClass());
        attr.setFundamentalType(column.getFundamentalType());
        attr.setDataSource(column.getMatchDestination());
        attr.setDescription(column.getDescription());
        attr.setIsSelected(selectedAttributeNames.contains(column.getAttrName()));
        attr.setIsPremium(column.isPremium());
        attr.setCategory(column.getCategory().getName());
        attr.setSubcategory(column.getSubcategory());
        attr.setIsInternal(Boolean.TRUE.equals(column.getCanInternalEnrich()));
        attr.setDataLicense(column.getDataLicense());
        return attr;
    }

}
