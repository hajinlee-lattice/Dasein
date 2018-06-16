package com.latticeengines.pls.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.AttrDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.AttrConfigService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

@Component("attrConfigService")
public class AttrConfigServiceImpl implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImpl.class);

    public static final String[] usageProperties = { ColumnSelection.Predefined.Segment.getName(),
            ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName() };
    private static final List<String> usagePropertyList = Arrays.asList(usageProperties);
    private static final HashMap<String, String> categoryToDisplayName = new HashMap<>();
    private static final HashMap<String, String> displayNameToCategory = new HashMap<>();
    private static final HashMap<String, String> usageToDisplayName = new HashMap<>();
    private static final HashMap<String, String> displayNameToUsage = new HashMap<>();
    private static final String defaultDisplayName = "Default Name";
    private static final String defaultDescription = "Default Description";

    static {
        // this map has all the maping for all premium attributes
        categoryToDisplayName.put("My Attributes", "My Account");
        categoryToDisplayName.put("Contact Attributes", "My Contact");
        categoryToDisplayName.put("Intent", "Intent");
        categoryToDisplayName.put("Technology Profile", "Technology Profile");
        categoryToDisplayName.put("Website Keywords", "Website Keyword");

        displayNameToCategory.put("My Account", "My Attributes");
        displayNameToCategory.put("My Contact", "Contact Attributes");
        displayNameToCategory.put("Intent", "Intent");
        displayNameToCategory.put("Technology Profile", "Technology Profile");
        displayNameToCategory.put("Website Keyword", "Website Keywords");

        usageToDisplayName.put(ColumnSelection.Predefined.Segment.getName(), "Segmentation");
        usageToDisplayName.put(ColumnSelection.Predefined.Enrichment.getName(), "Export");
        usageToDisplayName.put(ColumnSelection.Predefined.TalkingPoint.getName(), "Talking Points");
        usageToDisplayName.put(ColumnSelection.Predefined.CompanyProfile.getName(), "Company Profile");

        displayNameToUsage.put("Segmentation", ColumnSelection.Predefined.Segment.getName());
        displayNameToUsage.put("Export", ColumnSelection.Predefined.Enrichment.getName());
        displayNameToUsage.put("Talking Points", ColumnSelection.Predefined.TalkingPoint.getName());
        displayNameToUsage.put("Company Profile", ColumnSelection.Predefined.CompanyProfile.getName());
    }

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    private ActionService actionService;

    @Inject
    private UserService userService;

    @SuppressWarnings("unchecked")
    @Override
    public List<AttrConfigActivationOverview> getOverallAttrConfigActivationOverview() {
        List<AttrConfigActivationOverview> result = new ArrayList<>();
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
            categoryOverview.setDisplayName(mapCategoryToDisplayName(category.getName()));
            result.add(categoryOverview);
        }
        return result;
    }

    @Override
    public AttrConfigUsageOverview getOverallAttrConfigUsageOverview() {
        AttrConfigUsageOverview usageOverview = new AttrConfigUsageOverview();
        Map<String, Long> attrNums = new HashMap<>();
        Map<String, Map<String, Long>> selections = new LinkedHashMap<>();
        usageOverview.setAttrNums(attrNums);
        usageOverview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy
                .getAttrConfigOverview(MultiTenantContext.getTenantId(), null, Arrays.asList(usageProperties), true);
        log.info("map is " + map);

        for (String property : usagePropertyList) {
            Map<String, Long> detailedSelections = new HashMap<>();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())) {
                detailedSelections.put(AttrConfigUsageOverview.LIMIT, AttrConfigUsageOverview.defaultExportLimit);
            } else if (property.equals(ColumnSelection.Predefined.CompanyProfile.getName())) {
                detailedSelections.put(AttrConfigUsageOverview.LIMIT,
                        AttrConfigUsageOverview.defaultCompanyProfileLimit);
            }
            selections.put(mapUsageToDisplayName(property), detailedSelections);
            long num = 0L;
            for (String category : map.keySet()) {
                // For each category, update its total attrNums
                AttrConfigCategoryOverview<?> categoryOverview = map.get(category);
                attrNums.put(mapCategoryToDisplayName(category), categoryOverview.getTotalAttrs());

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
    public void updateActivationConfig(String categoryDisplayName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        String categoryName = mapDisplayNameToCategory(categoryDisplayName);
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForActivation(categoryName, request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
        createUpdateActivationActions(categoryName, request);
    }

    private void createUpdateActivationActions(String categoryName, AttrConfigSelectionRequest request) {
        if (request.getSelect() != null) {
            Action activationAction = new Action();
            activationAction.setActionInitiator(MultiTenantContext.getEmailAddress());
            activationAction.setType(ActionType.ATTRIBUTE_MANAGEMENT_ACTIVATION);
            AttrConfigLifeCycleChangeConfiguration ac = new AttrConfigLifeCycleChangeConfiguration();
            ac.setAttrNums((long) request.getSelect().size());
            ac.setCategoryName(categoryName);
            ac.setSubType(AttrConfigLifeCycleChangeConfiguration.SubType.ACTIVATION);
            activationAction.setActionConfiguration(ac);
            activationAction.setDescription(ac.serialize());
            actionService.create(activationAction);
        }
        if (request.getDeselect() != null) {
            Action deActivationAction = new Action();
            deActivationAction.setActionInitiator(MultiTenantContext.getEmailAddress());
            deActivationAction.setType(ActionType.ATTRIBUTE_MANAGEMENT_DEACTIVATION);
            AttrConfigLifeCycleChangeConfiguration ac = new AttrConfigLifeCycleChangeConfiguration();
            ac.setAttrNums((long) request.getDeselect().size());
            ac.setCategoryName(categoryName);
            ac.setSubType(AttrConfigLifeCycleChangeConfiguration.SubType.DEACTIVATION);
            deActivationAction.setActionConfiguration(ac);
            deActivationAction.setDescription(ac.serialize());
            actionService.create(deActivationAction);
        }
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForActivation(String categoryName, AttrConfigSelectionRequest request) {
        Category category = resolveCategory(categoryName);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        if (request.getSelect() != null) {
            for (String attr : request.getSelect()) {
                updateAttrConfigsForState(category, attrConfigs, attr, ColumnMetadataKey.State, AttrState.Active);
            }
        }
        if (request.getDeselect() != null) {
            verifyAccessLevel();
            for (String attr : request.getDeselect()) {
                updateAttrConfigsForState(category, attrConfigs, attr, ColumnMetadataKey.State, AttrState.Inactive);
            }
        }
        return attrConfigRequest;
    }

    private void verifyAccessLevel() {
        AccessLevel accessLevel = userService.getAccessLevel(MultiTenantContext.getTenantId(),
                MultiTenantContext.getEmailAddress());
        if (AccessLevel.SUPER_ADMIN != accessLevel && AccessLevel.INTERNAL_ADMIN != accessLevel) {
            throw new LedpException(LedpCode.LEDP_18185, new String[] { MultiTenantContext.getEmailAddress() });
        }
    }

    @VisibleForTesting
    void updateAttrConfigsForState(Category category, List<AttrConfig> attrConfigs, String attrName, String property,
            AttrState selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        config.setEntity(CategoryUtils.getEntity(category));
        AttrConfigProp<AttrState> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    private void updateAttrConfigs(Category category, List<AttrConfig> attrConfigs, String attrName, String property,
            Boolean selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        config.setEntity(CategoryUtils.getEntity(category));
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    @Override
    public void updateUsageConfig(String categoryDisplayName, String usageName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getTenantId();
        String categoryName = mapDisplayNameToCategory(categoryDisplayName);
        String usage = mapDisplayNameToUsage(usageName);
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForUsage(categoryName, usage, request);
        AttrConfigRequest saveResponse = cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForUsage(String categoryName, String property,
            AttrConfigSelectionRequest request) {
        Category category = resolveCategory(categoryName);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        if (request.getSelect() != null) {
            for (String attr : request.getSelect()) {
                updateAttrConfigs(category, attrConfigs, attr, property, Boolean.TRUE);
            }
        }
        if (request.getDeselect() != null) {
            for (String attr : request.getDeselect()) {
                updateAttrConfigs(category, attrConfigs, attr, property, Boolean.FALSE);
            }
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
    public AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryDisplayName) {
        String categoryName = mapDisplayNameToCategory(categoryDisplayName);
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getTenantId(), categoryName);
        return generateSelectionDetails(categoryName, attrConfigRequest, ColumnMetadataKey.State, false);
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryDisplayName, String usageName) {
        String categoryName = mapDisplayNameToCategory(categoryDisplayName);
        String property = mapDisplayNameToUsage(usageName);
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getTenantId(), categoryName);
        return generateSelectionDetails(categoryName, attrConfigRequest, property, true);
    }

    @SuppressWarnings("unchecked")
    AttrConfigSelectionDetail generateSelectionDetails(String categoryName, AttrConfigRequest attrConfigRequest,
            String property, boolean applyActivationFilter) {
        AttrConfigSelectionDetail attrConfigSelectionDetail = new AttrConfigSelectionDetail();
        long totalAttrs = 0L;
        long selected = 0L;

        List<SubcategoryDetail> subcategories = new ArrayList<>();
        // key: subcategory, value: index in the subcategories
        Map<String, Integer> subCategoryToIndexMap = new HashMap<>();
        attrConfigSelectionDetail.setSubcategories(subcategories);
        if (attrConfigRequest.getAttrConfigs() != null) {
            for (AttrConfig attrConfig : attrConfigRequest.getAttrConfigs()) {
                Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
                if (attrProps != null) {

                    boolean includeCurrentAttr = true;
                    if (applyActivationFilter) {
                        AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                                .get(ColumnMetadataKey.State);
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))) {
                            includeCurrentAttr = false;
                        }
                    }

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
                            if (subCategoryToIndexMap.containsKey(subcategory)) {
                                subcategoryDetail = subcategories.get(subCategoryToIndexMap.get(subcategory));
                            } else {
                                subCategoryToIndexMap.put(subcategory, subcategories.size());
                                subcategoryDetail = new SubcategoryDetail();
                                subcategoryDetail.setSubCategory(subcategory);
                                subcategories.add(subcategoryDetail);
                            }
                        } else {
                            log.warn(String.format("%s does not have subcategory property", attrConfig.getAttrName()));
                            continue;
                        }

                        // check designated property
                        AttrConfigProp<?> attrConfigProp = attrProps.get(property);
                        if (attrConfigProp != null) {
                            AttrDetail attrDetail = new AttrDetail();
                            attrDetail.setAttribute(attrConfig.getAttrName());
                            attrDetail.setDisplayName(getDisplayName(attrProps));
                            attrDetail.setDescription(getDescription(attrProps));

                            if (property.equals(ColumnMetadataKey.State)) {
                                // set the selection status
                                AttrState actualState = (AttrState) getActualValue(attrConfigProp);
                                if (AttrState.Active.equals(actualState)) {
                                    selected++;
                                    attrDetail.setSelected(true);
                                    subcategoryDetail.setSelected(true);
                                } else {
                                    attrDetail.setSelected(false);
                                }
                            } else if (usagePropertyList.contains(property)) {
                                Boolean actualState = (Boolean) getActualValue(attrConfigProp);
                                if (Boolean.TRUE.equals(actualState)) {
                                    selected++;
                                    attrDetail.setSelected(true);
                                    subcategoryDetail.setSelected(true);
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
                            subcategoryDetail.getAttributes().add(attrDetail);
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
        BusinessEntity entity = CategoryUtils.getEntity(resolveCategory(categoryName));
        attrConfigSelectionDetail.setEntity(entity);
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

    static String mapCategoryToDisplayName(String categoryName) {
        if (categoryToDisplayName.containsKey(categoryName)) {
            return categoryToDisplayName.get(categoryName);
        }
        return categoryName;
    }

    static String mapDisplayNameToCategory(String categoryDisplayName) {
        if (displayNameToCategory.containsKey(categoryDisplayName)) {
            return displayNameToCategory.get(categoryDisplayName);
        }
        return categoryDisplayName;
    }

    static String mapUsageToDisplayName(String usageName) {
        return usageToDisplayName.get(usageName);
    }

    static String mapDisplayNameToUsage(String usageDisplayName) {
        return displayNameToUsage.get(usageDisplayName);
    }

}
