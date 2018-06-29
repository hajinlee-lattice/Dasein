package com.latticeengines.pls.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.AttrConfigSelection;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.AttrDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.AttrConfigService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

@Component("attrConfigService")
public class AttrConfigServiceImpl implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImpl.class);

    public static final String[] usageProperties = { ColumnSelection.Predefined.Segment.getName(),
            ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName() };
    private static final List<String> usagePropertyList = Arrays.asList(usageProperties);
    private static final HashMap<String, String> usageToDisplayName = new HashMap<>();
    private static final HashMap<String, String> displayNameToUsage = new HashMap<>();
    private static final String defaultDisplayName = "Default Name";
    private static final String defaultDescription = "Default Description";

    static {
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

    @Inject
    private DataLakeService dataLakeService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfigStateOverview getOverallAttrConfigActivationOverview() {
        AttrConfigStateOverview overview = new AttrConfigStateOverview();
        List<AttrConfigSelection> selections = new ArrayList<>();
        overview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy.getAttrConfigOverview(
                MultiTenantContext.getShortTenantId(),
                Category.getPremiunCategories().stream().map(Category::getName).collect(Collectors.toList()),
                Arrays.asList(ColumnMetadataKey.State), false);
        for (Category category : Category.getPremiunCategories()) {
            AttrConfigCategoryOverview<AttrState> activationOverview = (AttrConfigCategoryOverview<AttrState>) map
                    .get(category.getName());
            AttrConfigSelection categoryOverview = new AttrConfigSelection();
            categoryOverview.setLimit(activationOverview.getLimit());
            categoryOverview.setTotalAttrs(activationOverview.getTotalAttrs());
            categoryOverview.setSelected(
                    activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active) != null
                            ? activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active)
                            : 0L);
            categoryOverview.setDisplayName(category.getName());
            selections.add(categoryOverview);
        }
        return overview;
    }

    @Override
    public AttrConfigUsageOverview getOverallAttrConfigUsageOverview() {
        AttrConfigUsageOverview usageOverview = new AttrConfigUsageOverview();
        Map<String, Long> attrNums = new HashMap<>();
        List<AttrConfigSelection> selections = new ArrayList<>();
        usageOverview.setAttrNums(attrNums);
        usageOverview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy.getAttrConfigOverview(
                MultiTenantContext.getShortTenantId(), null, Arrays.asList(usageProperties), true);
        log.info("map is " + map);

        for (String property : usagePropertyList) {
            AttrConfigSelection selection = new AttrConfigSelection();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())) {
                selection.setLimit(AttrConfigUsageOverview.defaultExportLimit);
            } else if (property.equals(ColumnSelection.Predefined.CompanyProfile.getName())) {
                selection.setLimit(AttrConfigUsageOverview.defaultCompanyProfileLimit);
            }
            selection.setDisplayName(mapUsageToDisplayName(property));
            selections.add(selection);
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
                selection.setSelected(num);
            }
        }

        return usageOverview;
    }

    @Override
    public void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getShortTenantId();
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
        if (CollectionUtils.isNotEmpty(request.getSelect())) {
            for (String attr : request.getSelect()) {
                updateAttrConfigsForState(category, attrConfigs, attr, ColumnMetadataKey.State, AttrState.Active);
            }
        }
        if (CollectionUtils.isNotEmpty(request.getDeselect())) {
            verifyAccessLevel();
            for (String attr : request.getDeselect()) {
                updateAttrConfigsForState(category, attrConfigs, attr, ColumnMetadataKey.State, AttrState.Inactive);
            }
        }
        return attrConfigRequest;
    }

    private void verifyAccessLevel() {
        AccessLevel accessLevel = userService.getAccessLevel(MultiTenantContext.getPLSTenantId(),
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
    public UpdateUsageResponse updateUsageConfig(String categoryName, String usageName,
            AttrConfigSelectionRequest request) {
        UpdateUsageResponse updateUsageResponse = new UpdateUsageResponse();
        String tenantId = MultiTenantContext.getShortTenantId();
        String usage = mapDisplayNameToUsage(usageName);
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForUsage(categoryName, usage, request);
        AttrConfigRequest saveResponse = cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest);
        if (saveResponse != null && saveResponse.hasWarning()) {
            List<AttrValidation> attrValidationList = saveResponse.getDetails().getValidations();
            if (request.getDeselect() != null && request.getDeselect().size() > 1) {
                // deactivate at subcategory level
                Set<String> attrSet = attrValidationList.stream().map(AttrValidation::getAttrName)
                        .collect(Collectors.toSet());
                updateUsageResponse.setMessage(attrSet.toString());
            } else if (request.getDeselect() != null && request.getDeselect().size() == 1) {
                // deactivate at attribute level
                Map<ImpactWarnings.Type, List<String>> warnings = attrValidationList.get(0).getImpactWarnings()
                        .getWarnings();
                updateUsageResponse.setMessage(warnings.toString());
            }
        }
        return updateUsageResponse;

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

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName) {
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getShortTenantId(), categoryName);
        return generateSelectionDetails(categoryName, attrConfigRequest, ColumnMetadataKey.State, false);
    }

    @Override
    public AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usageName) {
        String property = mapDisplayNameToUsage(usageName);
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getShortTenantId(), categoryName);
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

    static String mapUsageToDisplayName(String usageName) {
        return usageToDisplayName.get(usageName);
    }

    static String mapDisplayNameToUsage(String usageDisplayName) {
        return displayNameToUsage.get(usageDisplayName);
    }

    @Override
    public Map<String, AttributeStats> getStats(String categoryName, @PathVariable String subcatName) {
        Category cat = resolveCategory(categoryName);
        BusinessEntity entity = CategoryUtils.getEntity(cat);

        Map<String, StatsCube> cubes = dataLakeService.getStatsCubes();
        if (cubes.get(entity.name()) == null) {
            return Collections.<String, AttributeStats> emptyMap();
        }
        StatsCube cube = cubes.get(entity.name());
        Map<String, AttributeStats> stats = cube.getStatistics();
        if (MapUtils.isEmpty(stats)) {
            return Collections.<String, AttributeStats> emptyMap();
        }

        Set<String> attrsToRetain = getAttrsWithCatAndSubcat(entity, cat, subcatName);
        if (CollectionUtils.isEmpty(attrsToRetain)) {
            return Collections.<String, AttributeStats> emptyMap();
        }
        stats.entrySet().removeIf(e -> !attrsToRetain.contains(e.getKey()));
        return stats;
    }

    /**
     * Start with simple solution to get all metadata from cached decorated
     * metadata If having performance issue, metadata from datacloud could be
     * got from ColumnMetadataProxy
     */
    private Set<String> getAttrsWithCatAndSubcat(BusinessEntity entity, Category cat, String subcatName) {
        Set<String> attrs = new HashSet<>();
        List<ColumnMetadata> cms = servingStoreProxy
                .getDecoratedMetadataFromCache(MultiTenantContext.getCustomerSpace().toString(), entity);
        if (CollectionUtils.isEmpty(cms)) {
            return attrs;
        }
        cms.forEach(cm -> {
            if (cm.getCategory() == cat && subcatName.equalsIgnoreCase(cm.getSubcategory())) {
                attrs.add(cm.getAttrName());
            }
        });
        return attrs;
    }

    public static class UpdateUsageResponse {

        String message;

        public void setMessage(String msg) {
            message = msg;
        }

        public String getMessage() {
            return message;
        }

    }

}
