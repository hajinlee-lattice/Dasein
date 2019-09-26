package com.latticeengines.pls.service.impl;

import static j2html.TagCreator.b;
import static j2html.TagCreator.each;
import static j2html.TagCreator.li;
import static j2html.TagCreator.p;
import static j2html.TagCreator.ul;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
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
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings.Type;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.util.ApsGeneratorUtils;
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

    private static final List<String> usagePropertyList = Arrays.asList(ColumnSelection.Predefined.usageProperties);
    private static final HashMap<String, String> usageToDisplayName = new HashMap<>();
    private static final HashMap<String, String> displayNameToUsage = new HashMap<>();
    private static final String defaultDisplayName = "Default Name";
    private static final String defaultDescription = "Default Description";

    public static final String UPDATE_ACTIVATION_SUCCESS_TITLE = "Success! Your request has been submitted.";
    public static final String UPDATE_USAGE_SUCCESS_TITLE = "Your changes have been saved";
    public static final String UPDATE_WARNING_ATTRIBUTE_TITLE = "Attribute In Use";
    public static final String UPDATE_FAIL_TITLE = "Validation Error";
    public static final String UPDATE_FAIL_MSG = "There are validation errors";
    public static final String UPDATE_ACTIVATION_SUCCESSE_MSG = "<p>The action will be scheduled to process and analyze. You can track the status from the <a ui-sref='home.jobs.data'>Data P&A</a></p>";
    public static final String UPDATE_USAGE_FAIL_ATTRIBUTE_MSG = "This attribute is in use and cannot be disabled until the dependency has been removed.";
    public static final String UPDATE_ACTIVATION_FAIL_ATTRIBUTE_MSG = "This attribute is in use and cannot be deactivated until it is disabled from the following usecases.";
    public static final String UPDATE_WARNING_CATEGORY_TITLE = "Category In Use";
    public static final String UPDATE_USAGE_FAIL_CATEGORY_MSG = "This category is in use and cannot be disabled until the attributes have been removed.";
    public static final String UPDATE_ACTIVATION_FAIL_CATEGORY_MSG = "This category is in use and cannot be deactived until the attributes have been disabled.";
    public static final String UPDATE_WARNING_SUBCATEGORY_TITLE = "Sub-category In Use";
    public static final String UPDATE_USAGE_FAIL_SUBCATEGORY_MSG = "This sub-category is in use and cannot be disabled until the attributes have been removed.";
    public static final String UPDATE_ACTIVATION_FAIL_SUBCATEGORY_MSG = "This sub-category is in use and cannot be deactived until the attributes have been disabled.";

    static {
        usageToDisplayName.put(ColumnSelection.Predefined.Segment.getName(), "Segmentation");
        usageToDisplayName.put(ColumnSelection.Predefined.Enrichment.getName(), "Export");
        usageToDisplayName.put(ColumnSelection.Predefined.Model.getName(), "Modeling");
        usageToDisplayName.put(ColumnSelection.Predefined.TalkingPoint.getName(), "Talking Points");
        usageToDisplayName.put(ColumnSelection.Predefined.CompanyProfile.getName(), "Company Profile");

        displayNameToUsage.put("Segmentation", ColumnSelection.Predefined.Segment.getName());
        displayNameToUsage.put("Export", ColumnSelection.Predefined.Enrichment.getName());
        displayNameToUsage.put("Modeling", ColumnSelection.Predefined.Model.getName());
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

    @Inject
    private CommonTenantConfigService appTenantConfigService;

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfigStateOverview getOverallAttrConfigActivationOverview() {
        AttrConfigStateOverview overview = new AttrConfigStateOverview();
        List<AttrConfigSelection> selections = new ArrayList<>();
        overview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy.getAttrConfigOverview(
                MultiTenantContext.getShortTenantId(),
                Category.getPremiumCategories().stream().map(Category::getName).collect(Collectors.toList()),
                Collections.singletonList(ColumnMetadataKey.State), false);
        for (Category category : Category.getPremiumCategories()) {
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
        List<AttrConfigSelection> selections = new ArrayList<>();
        usageOverview.setSelections(selections);
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy
                .getAttrConfigOverview(MultiTenantContext.getShortTenantId(), null, usagePropertyList, true);
        log.info("map is " + map);

        for (String property : usagePropertyList) {
            AttrConfigSelection selection = new AttrConfigSelection();
            if (property.equals(ColumnSelection.Predefined.Enrichment.getName())
                    || property.equals(ColumnSelection.Predefined.CompanyProfile.getName())) {
                selection.setLimit((long) appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getShortTenantId(), property));
            }
            selection.setDisplayName(mapUsageToDisplayName(property));
            selections.add(selection);
            TreeMap<String, Long> categories = new TreeMap<>(Comparator.comparing(a -> //
            Objects.requireNonNull(Category.fromName(a)).getOrder()));
            selection.setCategories(categories);
            long selectedNum = 0L;
            for (String category : map.keySet()) {
                long categoryNum = 0L;
                // For each category, update its total attrNums
                AttrConfigCategoryOverview<?> categoryOverview = map.get(category);

                // Synthesize the properties for all the categories
                Map<?, Long> propertyDetail = categoryOverview.getPropSummary().get(property);
                if (propertyDetail != null) {
                    if (propertyDetail.get(Boolean.TRUE) != null) {
                        selectedNum += propertyDetail.get(Boolean.TRUE);
                    }
                    if (propertyDetail.values() != null) {
                        for (Long num : propertyDetail.values()) {
                            categoryNum += num;
                        }
                    }
                }
                selection.setSelected(selectedNum);
                categories.put(category, categoryNum);
            }
        }

        return usageOverview;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfigStateOverview getOverallAttrConfigNameOverview() {
        AttrConfigStateOverview overview = new AttrConfigStateOverview();
        List<AttrConfigSelection> selections = new ArrayList<>();
        overview.setSelections(selections);
        List<String> categories = Arrays.asList(Category.ACCOUNT_ATTRIBUTES.getName(),
                Category.CONTACT_ATTRIBUTES.getName());
        Map<String, AttrConfigCategoryOverview<?>> map = cdlAttrConfigProxy.getAttrConfigOverview(
                MultiTenantContext.getShortTenantId(), categories, Collections.singletonList(ColumnMetadataKey.State),
                false);
        for (String category : categories) {
            AttrConfigCategoryOverview<AttrState> activationOverview = (AttrConfigCategoryOverview<AttrState>) map
                    .get(category);
            AttrConfigSelection categoryOverview = new AttrConfigSelection();
            categoryOverview.setTotalAttrs(
                    activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active) != null
                            ? activationOverview.getPropSummary().get(ColumnMetadataKey.State).get(AttrState.Active)
                            : 0L);
            categoryOverview.setDisplayName(category);
            selections.add(categoryOverview);
        }
        return overview;
    }

    @Override
    public UIAction updateActivationConfig(String categoryName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getShortTenantId();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForActivation(categoryName, request);
        AttrConfigRequest saveResponse = cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest,
                AttrConfigUpdateMode.Activation);
        UIAction uiAction = processUpdateResponse(saveResponse, request, false);
        createUpdateActivationActions(categoryName, request);
        return uiAction;
    }

    private UIAction processUpdateResponse(AttrConfigRequest saveResponse, AttrConfigSelectionRequest request,
            boolean updateUsage) {
        UIAction uiAction = new UIAction();
        if (saveResponse != null) {
            if (saveResponse.hasError()) {
                uiAction.setTitle(UPDATE_FAIL_TITLE);
                uiAction.setView(View.Banner);
                uiAction.setStatus(Status.Error);
                uiAction.setMessage(generateErrorMsg(saveResponse));
                throw new UIActionException(uiAction, LedpCode.LEDP_18203);
            } else if (saveResponse.hasWarning()) {
                // parse the mode of the request
                int mode = parseModeForRequest(saveResponse, request);
                // form the uiAction and throw exception
                switch (mode) {
                case 0:
                    // attribute level
                    uiAction.setTitle(UPDATE_WARNING_ATTRIBUTE_TITLE);
                    uiAction.setView(View.Modal);
                    uiAction.setStatus(Status.Error);
                    uiAction.setMessage(generateAttrLevelMsg(saveResponse, updateUsage));
                    throw new UIActionException(uiAction, updateUsage ? LedpCode.LEDP_18190 : LedpCode.LEDP_18195);
                case 1:
                    // subcategory level
                    uiAction.setTitle(UPDATE_WARNING_SUBCATEGORY_TITLE);
                    uiAction.setView(View.Modal);
                    uiAction.setStatus(Status.Error);
                    uiAction.setMessage(generateSubcategoryLevelMsg(saveResponse, updateUsage));
                    throw new UIActionException(uiAction, updateUsage ? LedpCode.LEDP_18190 : LedpCode.LEDP_18195);
                case 2:
                    // category level
                    uiAction.setTitle(UPDATE_WARNING_CATEGORY_TITLE);
                    uiAction.setView(View.Modal);
                    uiAction.setStatus(Status.Error);
                    uiAction.setMessage(generateCategoryLevelMsg(saveResponse, updateUsage));
                    throw new UIActionException(uiAction, updateUsage ? LedpCode.LEDP_18190 : LedpCode.LEDP_18195);
                default:
                    return uiAction;
                }
            } else {
                if (updateUsage) {
                    uiAction.setTitle(UPDATE_USAGE_SUCCESS_TITLE);
                    uiAction.setView(View.Notice);
                    uiAction.setStatus(Status.Success);
                } else {
                    uiAction.setTitle(UPDATE_ACTIVATION_SUCCESS_TITLE);
                    uiAction.setView(View.Banner);
                    uiAction.setStatus(Status.Success);
                    uiAction.setMessage(generateUpdateActivationSuccessMsg());
                }
            }
        } else {
            String tenantId = MultiTenantContext.getShortTenantId();
            log.warn(String.format("Resposne for request %s of tenant %s is null.", request.toString(), tenantId));
        }
        return uiAction;
    }

    @VisibleForTesting
    String generateErrorMsg(AttrConfigRequest saveResponse) {
        List<AttrValidation> validations = saveResponse.getDetails().getValidations();
        Map<ValidationErrors.Type, MutablePair<Integer, Set<String>>> errorMap = new HashMap<>();
        validations.stream().forEach(validation -> {
            if (validation.getValidationErrors() != null) {
                Map<ValidationErrors.Type, List<String>> errors = validation.getValidationErrors().getErrors();
                for (Map.Entry<ValidationErrors.Type, List<String>> entry: errors.entrySet()) {
                    ValidationErrors.Type type = entry.getKey();
                    List<String> messages = entry.getValue();
                    errorMap.putIfAbsent(type, new MutablePair<>(0, new HashSet<>()));
                    MutablePair<Integer, Set<String>> pair = errorMap.get(type);
                    pair.setLeft(pair.getLeft() + 1);
                    pair.getRight().addAll(messages);
                }
            }
        });
        StringBuilder html = new StringBuilder();
        html.append(p(UPDATE_FAIL_MSG).render());
        for (ValidationErrors.Type type : errorMap.keySet()) {
            MutablePair<Integer, Set<String>> pair = errorMap.get(type);
            Set<String> details = pair.getRight();
            html.append((b(String.format(type.getMessage(), pair.getLeft()) + ":").render()));
            html.append(ul().with( //
                    each(details, attr -> //
                            li(attr))).render());
        }
        return html.toString();
    }

    private void createUpdateActivationActions(String categoryName, AttrConfigSelectionRequest request) {
        if (CollectionUtils.isNotEmpty(request.getSelect())) {
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
        if (CollectionUtils.isNotEmpty(request.getDeselect())) {
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
            verifyDeactivateAccessLevel();
            for (String attr : request.getDeselect()) {
                updateAttrConfigsForState(category, attrConfigs, attr, ColumnMetadataKey.State, AttrState.Inactive);
            }
        }
        return attrConfigRequest;
    }

    private void verifyDeactivateAccessLevel() {
        AccessLevel accessLevel = userService.getAccessLevel(MultiTenantContext.getPLSTenantId(),
                MultiTenantContext.getEmailAddress());
        if (AccessLevel.SUPER_ADMIN != accessLevel && AccessLevel.INTERNAL_ADMIN != accessLevel) {
            throw new LedpException(LedpCode.LEDP_18185, new String[] { MultiTenantContext.getEmailAddress() });
        }
    }

    private void verifyNameUpdateAccessLevel() {
        AccessLevel accessLevel = userService.getAccessLevel(MultiTenantContext.getPLSTenantId(),
                MultiTenantContext.getEmailAddress());
        if (AccessLevel.SUPER_ADMIN != accessLevel && AccessLevel.INTERNAL_ADMIN != accessLevel
                && AccessLevel.EXTERNAL_ADMIN != accessLevel) {
            throw new LedpException(LedpCode.LEDP_18204, new String[] { MultiTenantContext.getEmailAddress() });
        }
    }

    private void updateAttrConfigsForState(Category category, List<AttrConfig> attrConfigs, String attrName,
            String property, AttrState selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        // Only Premium Category can will be able to updated with State.
        // Take the first element of the list
        config.setEntity(CategoryUtils.getEntity(category).get(0));
        AttrConfigProp<AttrState> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    @VisibleForTesting
    void updateAttrConfigsForUsage(Category category, List<AttrConfig> attrConfigs, String attrName, String property,
            Boolean selectThisAttr) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);

        // This is for the convenience of UI. Currently UI does not have
        // the notion of entity, but only Category. In order to keep the same
        // API, PLS has to distinguish which entity it actually indicates based
        // on the attribute internal name pattern.
        if (Category.PRODUCT_SPEND.equals(category)) {
            if (ApsGeneratorUtils.isApsAttr(attrName)) {
                config.setEntity(BusinessEntity.AnalyticPurchaseState);
            } else {
                config.setEntity(BusinessEntity.PurchaseHistory);
            }
        } else {
            config.setEntity(CategoryUtils.getEntity(category).get(0));
        }
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        config.setAttrProps(ImmutableMap.of(property, enrichProp));
        attrConfigs.add(config);
    }

    private void updateAttrConfigsForNameAndDescription(Category category, List<AttrConfig> attrConfigs,
            AttrDetail request) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(request.getAttribute());
        // Only Accont and Contact can will be able to updated with Name &
        // Description. Take the first element of the list
        config.setEntity(CategoryUtils.getEntity(category).get(0));
        config.setAttrProps(new HashMap<>());
        if (request.getDisplayName() != null) {
            AttrConfigProp<String> nameProp = new AttrConfigProp<>();
            nameProp.setCustomValue(request.getDisplayName());
            config.getAttrProps().put(ColumnMetadataKey.DisplayName, nameProp);
        }
        if (request.getDescription() != null) {
            AttrConfigProp<String> desProp = new AttrConfigProp<>();
            desProp.setCustomValue(request.getDescription());
            config.getAttrProps().put(ColumnMetadataKey.Description, desProp);
        }

        attrConfigs.add(config);
    }

    @Override
    public UIAction updateUsageConfig(String categoryName, String usageName, AttrConfigSelectionRequest request) {
        String tenantId = MultiTenantContext.getShortTenantId();
        String usage = mapDisplayNameToUsage(usageName);
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForUsage(categoryName, usage, request);
        AttrConfigRequest saveResponse = cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest,
                AttrConfigUpdateMode.Usage);
        return processUpdateResponse(saveResponse, request, true);
    }

    @Override
    public void updateNameConfig(String categoryName, SubcategoryDetail request) {
        String tenantId = MultiTenantContext.getShortTenantId();
        verifyNameUpdateAccessLevel();
        AttrConfigRequest attrConfigRequest = generateAttrConfigRequestForName(categoryName, request);
        cdlAttrConfigProxy.saveAttrConfig(tenantId, attrConfigRequest, AttrConfigUpdateMode.Name);
    }

    @VisibleForTesting
    String generateUpdateActivationSuccessMsg() {
        StringBuilder html = new StringBuilder();
        html.append(UPDATE_ACTIVATION_SUCCESSE_MSG);
        return html.toString();
    }

    @VisibleForTesting
    String generateAttrLevelMsg(AttrConfigRequest saveResponse, boolean updateUsage) {
        AttrValidation attrValidation = saveResponse.getDetails().getValidations().get(0);
        Map<Type, List<String>> warnings = attrValidation.getImpactWarnings().getWarnings();
        StringBuilder html = new StringBuilder();
        if (updateUsage) {
            html.append(p(UPDATE_USAGE_FAIL_ATTRIBUTE_MSG).render());
            // PLS-9851 the order of the section is Segment -> Models -> Play ->
            // Talking Point -> Company Profile
            List<Type> sortedTypes = Arrays.asList(Type.IMPACTED_SEGMENTS, Type.IMPACTED_RATING_ENGINES,
                    Type.IMPACTED_RATING_MODELS, Type.IMPACTED_PLAYS, Type.IMPACTED_COMPANY_PROFILES);
            for (Type type : sortedTypes) {
                if (warnings.containsKey(type)) {
                    html.append(b(mapTypeToDisplayName(type)).render());
                    html.append(ul().with( //
                            each(warnings.get(type), entity -> //
                            li(entity))) //
                            .render());
                }
            }
        } else {
            html.append(p(UPDATE_ACTIVATION_FAIL_ATTRIBUTE_MSG).render());
            html.append(ul().with( //
                    each(warnings.get(Type.USAGE_ENABLED), entity -> //
                    li(mapUsageToDisplayName(entity)))) //
                    .render());
        }
        return html.toString();
    }

    @VisibleForTesting
    String generateSubcategoryLevelMsg(AttrConfigRequest saveResponse, boolean updateUsage) {
        Map<String, List<String>> subcategoryToAttrs = aggregateAttrValidations(saveResponse);
        StringBuilder html = new StringBuilder();
        if (updateUsage) {
            html.append(p(UPDATE_USAGE_FAIL_SUBCATEGORY_MSG).render());
        } else {
            html.append(p(UPDATE_ACTIVATION_FAIL_SUBCATEGORY_MSG).render());
        }
        subcategoryToAttrs.forEach((k, v) -> {
            html.append((b(k + ":").render()));
            html.append(ul().with( //
                    each(v, attr -> //
            li(attr))).render());
        });
        return html.toString();
    }

    @VisibleForTesting
    String generateCategoryLevelMsg(AttrConfigRequest saveResponse, boolean updateUsage) {
        Map<String, List<String>> subcategoryToAttrs = aggregateAttrValidations(saveResponse);
        StringBuilder html = new StringBuilder();
        if (updateUsage) {
            html.append(p(UPDATE_USAGE_FAIL_CATEGORY_MSG).render());
        } else {
            html.append(p(UPDATE_ACTIVATION_FAIL_CATEGORY_MSG).render());
        }
        subcategoryToAttrs.forEach((k, v) -> {
            html.append((b(k + ":").render()));
            html.append(ul().with( //
                    each(v, attr -> //
            li(attr))).render());
        });
        return html.toString();
    }

    private Map<String, List<String>> aggregateAttrValidations(AttrConfigRequest saveResponse) {
        List<AttrValidation> attrValidations = saveResponse.getDetails().getValidations();
        Map<String, List<String>> result = new HashMap<>();
        attrValidations.forEach(attrValidation -> {
            if (attrValidation.getSubcategory() == null) {
                log.warn(String.format("Attribute %s does not have valid subcategory info",
                        attrValidation.getAttrName()));
                return;
            }
            List<String> attrs = result.getOrDefault(attrValidation.getSubcategory(), new ArrayList<>());
            attrs.add(attrValidation.getAttrName());
            result.put(attrValidation.getSubcategory(), attrs);
        });
        return result;
    }

    private String mapTypeToDisplayName(ImpactWarnings.Type type) {
        switch (type) {
        case IMPACTED_SEGMENTS:
            return "Segment(s):";
        case IMPACTED_RATING_ENGINES:
            return "Model(s):";
        case IMPACTED_RATING_MODELS:
            return "RatingModel(s):";
        case IMPACTED_PLAYS:
            return "Campaign(s):";
        default:
            throw new IllegalArgumentException("This type conversion is not supported");
        }
    }

    private int parseModeForRequest(AttrConfigRequest saveResponse, AttrConfigSelectionRequest request) {
        // return value -- 0: attribute, 1: subcategory, 2: category
        if (CollectionUtils.isNotEmpty(request.getDeselect()) && request.getDeselect().size() == 1) {
            return 0;
        }
        List<AttrConfig> attrConfigs = saveResponse.getAttrConfigs();
        String subcategory = null;
        for (AttrConfig attrConfig : attrConfigs) {
            String currSubcategory = attrConfig.getPropertyFinalValue(ColumnMetadataKey.Subcategory, String.class);
            if (subcategory == null) {
                subcategory = currSubcategory;
            } else if (!subcategory.equals(currSubcategory)) {
                return 2;
            }
        }
        return 1;
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForUsage(String categoryName, String property,
            AttrConfigSelectionRequest request) {
        Category category = resolveCategory(categoryName);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        if (CollectionUtils.isNotEmpty(request.getSelect())) {
            for (String attr : request.getSelect()) {
                updateAttrConfigsForUsage(category, attrConfigs, attr, property, Boolean.TRUE);
            }
        }
        if (CollectionUtils.isNotEmpty(request.getDeselect())) {
            for (String attr : request.getDeselect()) {
                updateAttrConfigsForUsage(category, attrConfigs, attr, property, Boolean.FALSE);
            }
        }
        return attrConfigRequest;
    }

    @VisibleForTesting
    AttrConfigRequest generateAttrConfigRequestForName(String categoryName, SubcategoryDetail request) {
        Category category = resolveCategory(categoryName);
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        if (request != null && CollectionUtils.isNotEmpty(request.getAttributes())) {
            for (AttrDetail attr : request.getAttributes()) {
                updateAttrConfigsForNameAndDescription(category, attrConfigs, attr);
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
    public AttrConfigSelectionDetail getAttrConfigSelectionDetailForUsage(String categoryName, String usageName) {
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
                    /*
                     * DP-6630 For Activate/Deactivate page, hide attributes
                     * that are: Inactive and AllowCustomization=FALSE
                     *
                     * For Enable/Disable page, hide attributes that are:
                     * disabled and AllowCustomization=FALSE.
                     *
                     * PLS-11145 For Enable/Disable page, hide attributes that
                     * are: disabled and Deprecated
                     *
                     * 'onlyActivateAttrs=false' indicates it is
                     * Activate/Deactivate page, otherwise it is Usage
                     * Enable/Disable page
                     */
                    boolean includeCurrentAttr = true;
                    AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                            .get(ColumnMetadataKey.State);
                    if (applyActivationFilter) {
                        // PLS-10731 activation status does not apply to
                        // Modeling usage
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))
                                && !ColumnSelection.Predefined.Model.name().equals(property)) {
                            includeCurrentAttr = false;
                        }
                    } else {
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))
                                && !attrConfigProp.isAllowCustomization()) {
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
                        AttrConfigProp<?> configProp = attrProps.get(property);
                        if (configProp != null) {
                            /*
                             * For Enable/Disable page, hide attributes that
                             * are: disabled and AllowCustomization=FALSE.
                             *
                             * For Enable/Disable page, hide attributes that
                             * are: disabled and Deprecated
                             */
                            if (applyActivationFilter) {
                                if (Boolean.FALSE.equals(getActualValue(configProp))
                                        && (!configProp.isAllowCustomization()
                                                || Boolean.TRUE.equals(attrConfig.getShouldDeprecate()))) {
                                    continue;
                                }
                            }

                            AttrDetail attrDetail = new AttrDetail();
                            attrDetail.setAttribute(attrConfig.getAttrName());
                            attrDetail.setDisplayName(getDisplayName(attrProps));
                            attrDetail.setDescription(getDescription(attrProps));

                            if (property.equals(ColumnMetadataKey.State)) {
                                // set the selection status
                                AttrState actualState = (AttrState) getActualValue(configProp);
                                if (AttrState.Active.equals(actualState)) {
                                    selected++;
                                    attrDetail.setSelected(true);
                                    subcategoryDetail.setSelected(true);
                                } else {
                                    attrDetail.setSelected(false);
                                }
                            } else if (usagePropertyList.contains(property)) {
                                Boolean actualState = (Boolean) getActualValue(configProp);
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
                            if (!configProp.isAllowCustomization()) {
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
        attrConfigSelectionDetail.setTotalAttrs(totalAttrs);
        attrConfigSelectionDetail.setSelected(selected);
        return attrConfigSelectionDetail;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SubcategoryDetail getAttrConfigSelectionDetailForName(String categoryName) {
        SubcategoryDetail result = new SubcategoryDetail();
        result.setHasFrozenAttrs(null);
        result.setSelected(null);
        result.setTotalAttrs(null);
        List<AttrDetail> list = new ArrayList<>();
        result.setAttributes(list);
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy
                .getAttrConfigByCategory(MultiTenantContext.getShortTenantId(), categoryName);
        if (attrConfigRequest.getAttrConfigs() != null) {
            for (AttrConfig attrConfig : attrConfigRequest.getAttrConfigs()) {
                Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
                if (attrProps != null) {
                    boolean includeCurrentAttr = true;
                    AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                            .get(ColumnMetadataKey.State);
                    if (AttrState.Inactive.equals(getActualValue(attrConfigProp))) {
                        includeCurrentAttr = false;
                    }
                    if (includeCurrentAttr) {
                        AttrDetail attrDetail = new AttrDetail();
                        attrDetail.setAttribute(attrConfig.getAttrName());
                        attrDetail.setDefaultName(getDefaultName(attrProps));
                        attrDetail.setDisplayName(getDisplayName(attrProps));
                        attrDetail.setDescription(
                                getDescription(attrProps) == null ? StringUtils.EMPTY : getDescription(attrProps));
                        list.add(attrDetail);
                    }
                } else {
                    log.warn(String.format("%s does not have attrProps", attrConfig.getAttrName()));
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private String getDisplayName(Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<String> displayProp = (AttrConfigProp<String>) attrProps.get(ColumnMetadataKey.DisplayName);
        if (displayProp != null) {
            return (String) getActualValue(displayProp);
        }
        return defaultDisplayName;
    }

    @SuppressWarnings({ "unchecked" })
    private String getDefaultName(Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<String> displayProp = (AttrConfigProp<String>) attrProps.get(ColumnMetadataKey.DisplayName);
        if (displayProp != null) {
            return displayProp.getSystemValue();
        }
        return null;
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
        List<BusinessEntity> entities = CategoryUtils.getEntity(cat);

        Map<String, StatsCube> cubes = dataLakeService.getStatsCubes();
        if (MapUtils.isEmpty(cubes)) {
            return Collections.<String, AttributeStats> emptyMap();
        }
        Map<String, AttributeStats> result = new HashMap<>();
        for (BusinessEntity entity : entities) {
            if (cubes.get(entity.name()) == null) {
                continue;
            }
            StatsCube cube = cubes.get(entity.name());
            Map<String, AttributeStats> stats = cube.getStatistics();
            if (MapUtils.isEmpty(stats)) {
                continue;
            }
            Set<String> attrsToRetain = getAttrsWithCatAndSubcat(entity, cat, subcatName);
            if (CollectionUtils.isEmpty(attrsToRetain)) {
                continue;
            }
            stats.entrySet().removeIf(e -> !attrsToRetain.contains(e.getKey()));
            result.putAll(stats);
        }
        return result;
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

}
