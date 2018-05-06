package com.latticeengines.pls.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.AttrDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.pls.service.AttrConfigService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

@Component("attrConfigService")
public class AttrConfigServiceImpl implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImpl.class);

    public static final String[] usageProperties = { ColumnSelection.Predefined.Segment.getName(),
            ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName() };
    private static final Set<String> usagePropertySet = new HashSet<>(Arrays.asList(usageProperties));
    private static final String defaultDisplayName = "Default Name";
    private static final String defaultDescription = "Default Description";
    private static final long defaultExportLimit = 1000L;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfigActivationOverview getAttrConfigActivationOverview(Category category) {
        List<AttrConfigOverview<?>> list = cdlAttrConfigProxy.getAttrConfigOverview(MultiTenantContext.getTenantId(),
                category.getName(), ColumnMetadataKey.State);
        AttrConfigOverview<AttrState> attrCategoryOverview = (AttrConfigOverview<AttrState>) list.get(0);
        AttrConfigActivationOverview categoryOverview = new AttrConfigActivationOverview();
        categoryOverview.setCategory(attrCategoryOverview.getCategory());
        categoryOverview.setLimit(attrCategoryOverview.getLimit());
        categoryOverview.setTotalAttrs(attrCategoryOverview.getTotalAttrs());
        categoryOverview
                .setSelected(attrCategoryOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active));
        return categoryOverview;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, AttrConfigActivationOverview> getOverallAttrConfigActivationOverview() {
        Map<String, AttrConfigActivationOverview> result = new HashMap<>();
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy.getAttrConfigOverview(
                MultiTenantContext.getTenantId(),
                Category.getPremiunCategories().stream().map(Category::getName).collect(Collectors.toList()),
                Arrays.asList(ColumnMetadataKey.State), false);
        for (Category category : Category.getPremiunCategories()) {
            AttrConfigCategoryOverview<AttrState> activationOverview = (AttrConfigCategoryOverview<AttrState>) map
                    .get(category.getName());
            AttrConfigActivationOverview categoryOverview = new AttrConfigActivationOverview();
            categoryOverview.setLimit(activationOverview.getLimit());
            categoryOverview.setTotalAttrs(activationOverview.getTotalAttrs());
            categoryOverview.setSelected(
                    activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active) != null
                            ? activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active)
                            : 0L);
            result.put(category.getName(), categoryOverview);
        }
        return result;
    }

    @Override
    public AttrConfigUsageOverview getAttrConfigUsageOverview() {
        AttrConfigUsageOverview usageOverview = new AttrConfigUsageOverview();
        Map<String, Long> attrNums = new HashMap<>();
        Map<String, Map<String, Long>> selections = new HashMap<>();
        usageOverview.setAttrNums(attrNums);
        usageOverview.setSelections(selections);
        // TODO can be improved by multithreading
        int count = 0;
        for (String property : usageProperties) {
            List<AttrConfigOverview<?>> list = cdlAttrConfigProxy
                    .getAttrConfigOverview(MultiTenantContext.getTenantId(), null, property);
            log.info("list is " + list);
            Map<String, Long> detailedSelections = new HashMap<>();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())) {
                detailedSelections.put(AttrConfigUsageOverview.LIMIT, defaultExportLimit);
            }
            selections.put(property, detailedSelections);
            long num = 0L;
            for (AttrConfigOverview<?> attrConfigOverview : list) {
                // For each category, update its total attrNums
                if (count == 0) {
                    if (attrConfigOverview.getCategory().isPremium()) {
                        AttrConfigActivationOverview categoryOverview = getAttrConfigActivationOverview(
                                attrConfigOverview.getCategory());
                        attrNums.put(attrConfigOverview.getCategory().getName(), categoryOverview.getSelected());
                    } else {
                        attrNums.put(attrConfigOverview.getCategory().getName(), attrConfigOverview.getTotalAttrs());
                    }
                }

                // Synthesize the properties for all the categories
                Map<?, Long> propertyDetail = attrConfigOverview.getPropSummary().get(property);
                if (propertyDetail != null && propertyDetail.get(Boolean.TRUE) != null) {
                    num += propertyDetail.get(Boolean.TRUE);
                }
                detailedSelections.put(AttrConfigUsageOverview.SELECTED, num);
            }
            count++;
        }
        return usageOverview;
    }

    @Override
    public AttrConfigUsageOverview getOverallAttrConfigUsageOverview() {
        AttrConfigUsageOverview usageOverview = new AttrConfigUsageOverview();
        Map<String, Long> attrNums = new HashMap<>();
        Map<String, Map<String, Long>> selections = new HashMap<>();
        usageOverview.setAttrNums(attrNums);
        usageOverview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy
                .getAttrConfigOverview(MultiTenantContext.getTenantId(), null, Arrays.asList(usageProperties), true);
        log.info("map is " + map);

        for (String property : usageProperties) {
            Map<String, Long> detailedSelections = new HashMap<>();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())) {
                detailedSelections.put(AttrConfigUsageOverview.LIMIT, defaultExportLimit);
            }
            selections.put(property, detailedSelections);
            long num = 0L;
            for (String category : map.keySet()) {
                // For each category, update its total attrNums
                AttrConfigCategoryOverview<?> categoryOverview = map.get(category);
                attrNums.put(category, categoryOverview.getTotalAttrs());

                // Synthesize the properties for all the categories
                Map<?, Long> propertyDetail = categoryOverview.getPropSummary().get(property);
                if (propertyDetail != null && propertyDetail.get(Boolean.TRUE) != null) {
                    num += propertyDetail.get(Boolean.TRUE);
                }
                detailedSelections.put(AttrConfigUsageOverview.SELECTED, num);
            }
        }

        return usageOverview;
    }

    @Override
    public void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForActivation(request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForActivation(AttrConfigSelectionRequest request) {
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        if (request.getSelect() != null) {
            for (String attr : request.getSelect()) {
                updateAttrConfigs(attrConfigs, attr, ColumnMetadataKey.State, Boolean.TRUE);
            }
        }
        if (request.getDeselect() != null) {
            for (String attr : request.getDeselect()) {
                updateAttrConfigs(attrConfigs, attr, ColumnMetadataKey.State, Boolean.FALSE);
            }
        }
        return attrConfigRequest;
    }

    private void updateAttrConfigs(List<AttrConfig> attrConfigs, String attrName, String property,
            Boolean selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        config.setEntity(BusinessEntity.Account);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    @Override
    public void updateUsageConfig(String categoryName, String usage, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForUsage(usage, request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForUsage(String usage, AttrConfigSelectionRequest request) {
        String property = translateUsageToProperty(usage);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        for (String attr : request.getSelect()) {
            updateAttrConfigs(attrConfigs, attr, property, Boolean.TRUE);
        }
        for (String attr : request.getDeselect()) {
            updateAttrConfigs(attrConfigs, attr, property, Boolean.FALSE);
        }
        return attrConfigRequest;
    }

    @VisibleForTesting
    String translateUsageToProperty(String usage) {
        if (usage.equalsIgnoreCase("SEGMENTATION")) {
            return ColumnSelection.Predefined.Segment.getName();
        } else if (usage.equalsIgnoreCase("EXPORT")) {
            return ColumnSelection.Predefined.Enrichment.getName();
        } else if (usage.equalsIgnoreCase("TALKING POINTS")) {
            return ColumnSelection.Predefined.TalkingPoint.getName();
        } else if (usage.equalsIgnoreCase("COMPANY PROFILE")) {
            return ColumnSelection.Predefined.CompanyProfile.getName();
        } else if (usage.equalsIgnoreCase(ColumnMetadataKey.State)) {
            return ColumnMetadataKey.State;
        } else {
            throw new IllegalArgumentException(String.format("%s is not a valid usage", usage));
        }
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName) {
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getTenantId(), categoryName);
        return generateSelectionDetails(attrConfigRequest, ColumnMetadataKey.State, false, false);
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usage) {
        String property = translateUsageToProperty(usage);
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getTenantId(), categoryName);
        return generateSelectionDetails(attrConfigRequest, property, true, true);
    }

    @VisibleForTesting
    AttrConfigSelectionDetail generateSelectionDetails(AttrConfigRequest attrConfigRequest, String property,
            boolean applyActivationFilter) {
        return generateSelectionDetails(attrConfigRequest, property, applyActivationFilter, false);
    }

    @SuppressWarnings("unchecked")
    AttrConfigSelectionDetail generateSelectionDetails(AttrConfigRequest attrConfigRequest, String property,
            boolean applyActivationFilter, boolean filterOutNonCustomizedAttrs) {
        AttrConfigSelectionDetail attrConfigSelectionDetail = new AttrConfigSelectionDetail();
        long totalAttrs = 0L;
        long selected = 0L;

        Map<String, SubcategoryDetail> subcategories = new HashMap<>();
        attrConfigSelectionDetail.setSubcategories(subcategories);
        if (attrConfigRequest.getAttrConfigs() != null) {
            for (AttrConfig attrConfig : attrConfigRequest.getAttrConfigs()) {
                Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
                if (attrProps != null) {

                    boolean includeCurrentAttr = true;
                    if (applyActivationFilter) {
                        AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                                .get(ColumnMetadataKey.State);
                        if (getActualValue(attrConfigProp) == null
                                || getActualValue(attrConfigProp) == AttrState.Inactive) {
                            includeCurrentAttr = false;
                        }
                    }
                    // PLS-8199 --------------
                    if (includeCurrentAttr && filterOutNonCustomizedAttrs) {
                        if (usagePropertySet.contains(property)) {
                            AttrConfigProp<Boolean> attrConfigProp = (AttrConfigProp<Boolean>) attrProps.get(property);
                            if (!attrConfigProp.isAllowCustomization()
                                    && (Boolean) attrConfigProp.getSystemValue() == Boolean.FALSE) {
                                log.warn(String.format("property %s by default cannot be shown in UI", property));
                                includeCurrentAttr = false;
                            }
                        } else {
                            log.warn(String.format("Current property %s cannot apply filterOutNonCustomizedAttrs",
                                    property));
                        }
                    }
                    // --------------

                    if (includeCurrentAttr) {
                        // check subcategory property
                        AttrConfigProp<String> subcategoryProp = (AttrConfigProp<String>) attrProps
                                .get(ColumnMetadataKey.Subcategory);
                        SubcategoryDetail subcategoryDetail = null;
                        String subcategory = null;
                        if (subcategoryProp != null) {
                            subcategory = (String) getActualValue(subcategoryProp);
                            if (subcategory == null) {
                                log.warn(String.format("subcategory value for %s is null", attrConfig.getAttrName()));
                                continue;
                            }
                            subcategoryDetail = subcategories.getOrDefault(subcategory, new SubcategoryDetail());
                        } else {
                            log.warn(String.format("%s does not have subcategory property", attrConfig.getAttrName()));
                            continue;
                        }

                        // check designated property
                        AttrConfigProp<?> attrConfigProp = attrProps.get(property);
                        if (attrConfigProp != null) {
                            AttrDetail attrDetail = new AttrDetail();
                            attrDetail.setDisplayName(getDisplayName(attrProps));
                            attrDetail.setDescription(getDescription(attrProps));

                            if (property.equals(ColumnMetadataKey.State)) {
                                // set the selection status
                                AttrState actualState = (AttrState) getActualValue(attrConfigProp);
                                if (AttrState.Active == actualState) {
                                    selected++;
                                    attrDetail.setSelected(true);
                                    subcategoryDetail.setSelected(subcategoryDetail.getSelected() + 1);
                                } else {
                                    attrDetail.setSelected(false);
                                }
                            } else if (usagePropertySet.contains(property)) {
                                Boolean actualState = (Boolean) getActualValue(attrConfigProp);
                                if (actualState) {
                                    selected++;
                                    attrDetail.setSelected(true);
                                    subcategoryDetail.setSelected(subcategoryDetail.getSelected() + 1);
                                } else {
                                    attrDetail.setSelected(false);
                                }
                            } else {
                                log.warn(String.format("Current property %s is not supported for selection yet",
                                        property));
                                continue;
                            }

                            // set the frozen status for attr & subcategory
                            if (!attrConfigProp.isAllowCustomization()) {
                                attrDetail.setIsFrozen(true);
                                subcategoryDetail.setHasFrozenAttrs(true);
                            }

                            subcategoryDetail.setTotalAttrs(subcategoryDetail.getTotalAttrs() + 1);
                            subcategoryDetail.getAttributes().put(attrConfig.getAttrName(), attrDetail);
                            subcategories.put(subcategory, subcategoryDetail);
                            totalAttrs++;
                        } else {
                            log.warn(String.format("%s does not have property %s", attrConfig.getAttrName(), property));
                        }
                    }

                } else {
                    log.warn(String.format("%s does not have attrProps", attrConfig.getAttrName()));
                }
            }
        }
        // TODO change to real limit
        // attrConfigSelectionDetail.setLimit(500L);
        attrConfigSelectionDetail.setTotalAttrs(totalAttrs);
        attrConfigSelectionDetail.setSelected(selected);
        attrConfigSelectionDetail.setEntity(BusinessEntity.Account);
        return attrConfigSelectionDetail;
    }

    @SuppressWarnings("unchecked")
    private String getDisplayName(Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<String> displayProp = (AttrConfigProp<String>) attrProps.get(ColumnMetadataKey.DisplayName);
        if (displayProp != null) {
            return (String) getActualValue(displayProp);
        }
        return defaultDisplayName;
    }

    @SuppressWarnings("unchecked")
    private String getDescription(Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<String> descriptionProp = (AttrConfigProp<String>) attrProps.get(ColumnMetadataKey.Description);
        if (descriptionProp != null) {
            return (String) getActualValue(descriptionProp);
        }
        return defaultDescription;
    }

    private Category resolveCategory(String categoryName) {
        Category category = Category.fromName(categoryName);
        if (category == null) {
            throw new IllegalArgumentException("Cannot parse category " + categoryName);
        }
        return category;
    }

    <T extends Serializable> Object getActualValue(AttrConfigProp<T> configProp) {
        if (configProp.isAllowCustomization()) {
            if (configProp.getCustomValue() != null) {
                return configProp.getCustomValue();
            }
        }
        return configProp.getSystemValue();
    }

}
