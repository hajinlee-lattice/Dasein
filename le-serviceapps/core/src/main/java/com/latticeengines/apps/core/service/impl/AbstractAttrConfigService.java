package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.util.AttrTypeResolver;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.util.CategoryUtils;

public abstract class AbstractAttrConfigService implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AbstractAttrConfigService.class);

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private AttrValidationService attrValidationService;

    @Inject
    private LimitationValidator limitationValidator;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected abstract List<ColumnMetadata> getSystemMetadata(Category category);

    private static final long DEFAULT_LIMIT = 500L;

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render) {
        String tenantId = MultiTenantContext.getTenantId();
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(entity);
            if (render) {
                renderedList = render(columns, customConfig);
            } else {
                renderedList = customConfig;
            }
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public List<AttrConfigOverview<?>> getAttrConfigOverview(Category category, String propertyName) {
        List<AttrConfigOverview<?>> results = new ArrayList<>();
        log.info("category is " + category);
        if (category == null) {
            // get attrConfigOverview for each category. can be improved by
            // multithreading
            for (Category ele : Category.values()) {
                results.add(getAttrConfigOverview(getRenderedList(ele), ele, propertyName));
            }
        } else {
            results.add(getAttrConfigOverview(getRenderedList(category), category, propertyName));
        }
        return results;
    }

    @Override
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean activeOnly) {
        ConcurrentMap<String, AttrConfigCategoryOverview<?>> attrConfigOverview = new ConcurrentHashMap<>();
        log.info("categories are" + categories + ", propertyNames are " + propertyNames + ", activeOnly " + activeOnly);
        final Tenant tenant = MultiTenantContext.getTenant();

        List<Runnable> runnables = new ArrayList<>();
        categories.forEach(category -> {
            Runnable runnable = () -> {
                MultiTenantContext.setTenant(tenant);
                AttrConfigCategoryOverview<?> attrConfigCategoryOverview = getAttrConfigOverview(
                        getRenderedList(category), category, propertyNames, activeOnly);
                attrConfigOverview.put(category.getName(), attrConfigCategoryOverview);
            };
            runnables.add(runnable);
        });
        // fork join execution
        ExecutorService threadPool = ThreadPoolUtils.getFixedSizeThreadPool("attr-config-overview", 4);
        ThreadPoolUtils.runRunnablesInParallel(threadPool, runnables, 30, 1);
        new Thread(threadPool::shutdown).run();
        return attrConfigOverview;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    <T extends Serializable> AttrConfigCategoryOverview<T> getAttrConfigOverview(@NonNull List<AttrConfig> renderedList,
            Category category, List<String> propertyNames, boolean onlyActiveAttrs) {
        AttrConfigCategoryOverview<T> overview = new AttrConfigCategoryOverview<T>();
        Map<String, Map<T, Long>> propSummary = new HashMap<>();
        overview.setPropSummary(propSummary);
        if (category.isPremium()) {
            switch (category) {
            case INTENT:
                overview.setLimit((long) limitationValidator.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getTenantId(), DataLicense.BOMBORA));
                break;
            case TECHNOLOGY_PROFILE:
                overview.setLimit((long) limitationValidator.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getTenantId(), DataLicense.HG));
                break;
            case ACCOUNT_ATTRIBUTES:
                overview.setLimit(DEFAULT_LIMIT);
                break;
            case CONTACT_ATTRIBUTES:
                overview.setLimit(DEFAULT_LIMIT);
                break;
            default:
                log.warn("Unsupported" + category);
                break;
            }
        }

        // TODO multithreading
        log.info("Trying to get detailed config for " + category + " with properties: " + propertyNames);
        for (String propertyName : propertyNames) {
            Map<T, Long> valueNumberMap = new HashMap<>();
            propSummary.put(propertyName, valueNumberMap);
            long totalAttrs = 0;
            for (AttrConfig attrConfig : renderedList) {
                Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
                if (attrProps != null) {
                    boolean includeCurrentAttr = true;
                    if (onlyActiveAttrs) {
                        AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                                .get(ColumnMetadataKey.State);
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))) {
                            includeCurrentAttr = false;
                        }
                    }
                    if (includeCurrentAttr) {
                        AttrConfigProp<?> configProp = attrProps.get(propertyName);
                        String attrName = attrConfig.getAttrName();
                        if (configProp != null) {
                            Object actualValue;
                            try {
                                actualValue = getActualValue(configProp);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to get the actual value of " + propertyName
                                        + " in attribute " + attrName + ": " + JsonUtils.serialize(configProp), e);
                            }
                            if (actualValue == null) {
                                log.warn(String.format("configProp %s does not have proper value", configProp));
                                continue;
                            }
                            Long count = valueNumberMap.getOrDefault(actualValue, 0L);
                            valueNumberMap.put((T) actualValue, count + 1);
                            totalAttrs++;
                        } else {
                            log.warn(String.format("Attr %s does not have property %s", attrConfig.getAttrName(),
                                    propertyName));
                        }
                    }
                } else {
                    log.warn(String.format("Attr %s does not have properties", attrConfig.getAttrName()));
                }
            }
            overview.setTotalAttrs(totalAttrs);
        }
        return overview;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    <T extends Serializable> AttrConfigOverview<T> getAttrConfigOverview(@NonNull List<AttrConfig> renderedList,
            Category category, String propertyName) {
        AttrConfigOverview<T> overview = new AttrConfigOverview<T>();
        Map<String, Map<T, Long>> propSummary = new HashMap<>();
        overview.setPropSummary(propSummary);
        Map<T, Long> valueNumberMap = new HashMap<>();
        propSummary.put(propertyName, valueNumberMap);
        overview.setCategory(category);
        overview.setTotalAttrs((long) renderedList.size());
        if (category.isPremium()) {
            switch (category) {
            case INTENT:
                overview.setLimit((long) limitationValidator.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getTenantId(), DataLicense.BOMBORA));
                break;
            case TECHNOLOGY_PROFILE:
                overview.setLimit((long) limitationValidator.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getTenantId(), DataLicense.HG));
            case ACCOUNT_ATTRIBUTES:
                overview.setLimit(DEFAULT_LIMIT);
                break;
            case CONTACT_ATTRIBUTES:
                overview.setLimit(DEFAULT_LIMIT);
                break;
            default:
                log.warn("Unsupported" + category);
                break;
            }
        }

        for (AttrConfig attrConfig : renderedList) {
            Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
            AttrConfigProp<?> configProp = attrProps.get(propertyName);
            if (configProp != null) {
                Object actualValue = getActualValue(configProp);
                if (actualValue == null) {
                    log.warn(String.format("configProp %s does not have proper value", configProp));
                    continue;
                }
                Long count = valueNumberMap.get(actualValue);
                if (count == null) {
                    valueNumberMap.put((T) actualValue, 1L);
                } else {
                    valueNumberMap.put((T) actualValue, count + 1);
                }
            }
        }
        return overview;
    }

    @VisibleForTesting
    <T extends Serializable> Object getActualValue(AttrConfigProp<T> configProp) {
        if (configProp.isAllowCustomization() && configProp.getCustomValue() != null) {
            return configProp.getCustomValue();
        }
        return configProp.getSystemValue();
    }

    @Override
    public List<AttrConfig> getRenderedList(Category category) {
        List<AttrConfig> renderedList;
        String tenantId = MultiTenantContext.getTenantId();
        BusinessEntity entity = CategoryUtils.getEntity(category);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(category);
            Set<String> columnsInSystem = columns.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
            List<AttrConfig> customConfigInCategory = customConfig.stream() //
                    .filter(attrConfig -> category
                            .equals(attrConfig.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class))
                            || columnsInSystem.contains(attrConfig.getAttrName()))
                    .collect(Collectors.toList());
            renderedList = render(columns, customConfigInCategory);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public AttrConfigRequest validateRequest(AttrConfigRequest request) {
        ValidationDetails details = attrValidationService.validate(request.getAttrConfigs());
        request.setDetails(details);
        return request;
    }

    @Override
    public AttrConfigRequest saveRequest(AttrConfigRequest request) {
        AttrConfigRequest toReturn;

        // split by entity
        List<AttrConfig> attrConfigs = request.getAttrConfigs();
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrps = new HashMap<>();
        attrConfigs.forEach(attrConfig -> {
            BusinessEntity entity = attrConfig.getEntity();
            if (!attrConfigGrps.containsKey(entity)) {
                attrConfigGrps.put(entity, new ArrayList<>());
            }
            attrConfigGrps.get(entity).add(attrConfig);
        });

        if (MapUtils.isEmpty(attrConfigGrps)) {
            toReturn = request;
        } else {
            String tenantId = MultiTenantContext.getTenantId();
            Map<String, List<String>> diffProperties = mergeConfigWithExisting(tenantId, attrConfigGrps);

            List<AttrConfig> renderedList;
            Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = new HashMap<>();

            if (attrConfigGrps.size() == 1) {
                BusinessEntity entity = new ArrayList<>(attrConfigGrps.keySet()).get(0);
                renderedList = renderForEntity(attrConfigs, entity);
                attrConfigGrpsForTrim.put(entity, renderedList);
            } else {
                log.info("Saving attr configs for " + attrConfigGrps.size() + " entities in parallel.");
                // distribute to tasklets
                final Tenant tenant = MultiTenantContext.getTenant();
                List<Callable<List<AttrConfig>>> callables = new ArrayList<>();
                attrConfigGrps.forEach((entity, configList) -> {
                    Callable<List<AttrConfig>> callable = () -> {
                        MultiTenantContext.setTenant(tenant);
                        List<AttrConfig> renderedConfigList = renderForEntity(configList, entity);
                        attrConfigGrpsForTrim.put(entity, renderedConfigList);
                        return renderedConfigList;
                    };
                    callables.add(callable);
                });

                // fork join execution
                ExecutorService threadPool = ThreadPoolUtils.getForkJoinThreadPool("attr-config", 4);
                List<List<AttrConfig>> lists = ThreadPoolUtils.runCallablesInParallel(threadPool, callables, 30, 1);
                new Thread(threadPool::shutdown).run();
                renderedList = lists.stream().flatMap(list -> {
                    if (CollectionUtils.isNotEmpty(list)) {
                        return list.stream();
                    } else {
                        return Stream.empty();
                    }
                }).collect(Collectors.toList());
            }

            log.info("rendered List" + JsonUtils.serialize(renderedList));
            toReturn = new AttrConfigRequest();
            toReturn.setAttrConfigs(renderedList);
            AttrConfigRequest validated = validateRequest(toReturn);
            if (validated.hasError()) {
                throw new IllegalArgumentException("Request has validation errors, cannot be saved: "
                        + JsonUtils.serialize(validated.getDetails()));
            }

            log.info("AttrConfig before saving is " + JsonUtils.serialize(request));
            // trim and save
            attrConfigGrpsForTrim.forEach((entity, configList) -> {
                attrConfigEntityMgr.save(MultiTenantContext.getTenantId(), entity, trim(configList));
            });

            // try {
            // registerAction(diffProperties);
            // } catch (Exception e) {
            // log.error("Cannot register action to action table: " +
            // e.getMessage());
            // }
        }

        return toReturn;
    }

    /**
     * Merge with existing configs in Document DB
     */
    private Map<String, List<String>> mergeConfigWithExisting(String tenantId,
            Map<BusinessEntity, List<AttrConfig>> attrConfigGrps) {
        // record the changes between user selected props and existing config
        // props
        Map<String, List<String>> diffProperties = new HashMap<>();
        // attrConfig whose props is empty after merging
        List<AttrConfigEntity> toDeleteEntities = new ArrayList<>();

        attrConfigGrps.forEach((entity, configList) -> {
            List<AttrConfigEntity> existingConfigEntities = attrConfigEntityMgr.findAllByTenantAndEntity(tenantId,
                    entity);

            Map<String, AttrConfig> existingMap = new HashMap<>();
            Map<String, AttrConfigEntity> existingEntityMap = new HashMap<>();
            existingConfigEntities.forEach(configEntity -> {
                AttrConfig config = configEntity.getDocument();
                if (config != null) {
                    existingMap.put(config.getAttrName(), config);
                    existingEntityMap.put(config.getAttrName(), configEntity);
                }
            });
            for (AttrConfig config : configList) {
                String attrName = config.getAttrName();
                AttrConfig existingConfig = existingMap.get(attrName);
                if (existingConfig != null) {
                    // write user newly selected prop
                    config.getAttrProps().forEach((propName, value) -> {
                        if (!existingConfig.getAttrProps().containsKey(propName)) {
                            List<String> diffList = diffProperties.getOrDefault(attrName, new ArrayList<>());
                            diffList.add(propName);
                            diffProperties.put(attrName, diffList);
                        }
                    });
                    // write user changed prop
                    existingConfig.getAttrProps().forEach((propName, propValue) -> {
                        AttrConfigProp<?> prop = config.getAttrProps().get(propName);
                        if (prop != null && propValue != null && propValue.getCustomValue() != prop.getCustomValue()) {
                            List<String> diffList = diffProperties.getOrDefault(attrName, new ArrayList<>());
                            diffList.add(propName);
                            diffProperties.put(attrName, diffList);
                        }
                        if (!config.getAttrProps().containsKey(propName)) {
                            config.getAttrProps().put(propName, propValue);
                        }
                    });

                    // count the empty AttrConfig
                    boolean isEmpty = true;
                    for (Map.Entry<String, AttrConfigProp<?>> entry : config.getAttrProps().entrySet()) {
                        AttrConfigProp<?> val = entry.getValue();
                        if (val.getCustomValue() != null) {
                            isEmpty = false;
                            break;
                        }
                    }
                    if (isEmpty) {
                        toDeleteEntities.add(existingEntityMap.get(attrName));
                    }
                }
            }
        });
        attrConfigEntityMgr.deleteConfigs(toDeleteEntities);
        return diffProperties;
    }

    /**
     * Input AttrConfig may only have partial AttrProps
     */
    private List<AttrConfig> renderForEntity(List<AttrConfig> configList, BusinessEntity entity) {
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Set<String> attrNames = configList.stream().map(AttrConfig::getAttrName).collect(Collectors.toSet());
            List<ColumnMetadata> systemMds = getSystemMetadata(entity);
            List<ColumnMetadata> columns = systemMds.stream() //
                    .filter(cm -> attrNames.contains(cm.getAttrName())) //
                    .collect(Collectors.toList());
            renderedList = render(columns, configList);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs in entity %s for saving", count, entity);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @SuppressWarnings("unchecked")
    public List<AttrConfig> render(List<ColumnMetadata> systemMetadata, List<AttrConfig> customConfig) {
        if (systemMetadata == null) {
            return customConfig;
        } else if (customConfig == null) {
            customConfig = new ArrayList<>();
        }
        Map<String, AttrConfig> map = new HashMap<>();
        for (AttrConfig config : customConfig) {
            map.put(config.getAttrName(), config);
        }
        for (ColumnMetadata metadata : systemMetadata) {
            AttrType type = AttrTypeResolver.resolveType(metadata);
            AttrSubType subType = AttrTypeResolver.resolveSubType(metadata);
            if (AttrType.Internal.equals(type)) {
                continue;
            }
            AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(type, subType, metadata.getEntity());
            if (attrSpec == null) {
                log.warn(String.format("Cannot get Attr Specification for Type %s, SubType %s", type.name(),
                        subType.name()));
            }
            AttrConfig mergeConfig = map.get(metadata.getAttrName());
            if (mergeConfig == null) {
                mergeConfig = new AttrConfig();
                mergeConfig.setAttrName(metadata.getAttrName());
                mergeConfig.setAttrProps(new HashMap<>());
            }
            mergeConfig.setAttrType(type);
            mergeConfig.setAttrSubType(subType);
            mergeConfig.setEntity(metadata.getEntity());
            mergeConfig.setDataLicense(metadata.getDataLicense());

            Map<String, AttrConfigProp<?>> attrProps = mergeConfig.getAttrProps();
            if (attrProps == null) {
                attrProps = new HashMap<>();
            }

            AttrConfigProp<Category> cateProp = (AttrConfigProp<Category>) attrProps
                    .getOrDefault(ColumnMetadataKey.Category, new AttrConfigProp<Category>());
            cateProp.setSystemValue(metadata.getCategory());
            cateProp.setAllowCustomization(attrSpec == null || attrSpec.categoryNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.Category, cateProp);

            AttrConfigProp<String> subCateProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Subcategory, new AttrConfigProp<String>());
            subCateProp.setSystemValue(metadata.getSubcategory());
            subCateProp.setAllowCustomization(attrSpec == null || attrSpec.categoryNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.Subcategory, subCateProp);

            AttrConfigProp<AttrState> stateProp = (AttrConfigProp<AttrState>) attrProps
                    .getOrDefault(ColumnMetadataKey.State, new AttrConfigProp<AttrState>());
            AttrState state;
            if (metadata.getAttrState() == null) {
                state = AttrState.Active;
            } else {
                state = metadata.getAttrState();
            }
            stateProp.setSystemValue(state);
            stateProp.setAllowCustomization(true);
            mergeConfig.putProperty(ColumnMetadataKey.State, stateProp);
            modifyDeprecatedAttrState(mergeConfig, metadata);

            AttrConfigProp<String> displayNameProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.DisplayName, new AttrConfigProp<String>());
            displayNameProp.setSystemValue(metadata.getDisplayName());
            displayNameProp.setAllowCustomization(attrSpec == null || attrSpec.displayNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.DisplayName, displayNameProp);

            AttrConfigProp<String> descriptionProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Description, new AttrConfigProp<String>());
            descriptionProp.setSystemValue(metadata.getDescription());
            descriptionProp.setAllowCustomization(attrSpec == null || attrSpec.descriptionChange());
            mergeConfig.putProperty(ColumnMetadataKey.Description, descriptionProp);

            for (ColumnSelection.Predefined group : ColumnSelection.Predefined.values()) {
                AttrConfigProp<Boolean> usageProp;
                switch (group) {
                case CompanyProfile:
                case Enrichment:
                case Model:
                case Segment:
                case TalkingPoint:
                    usageProp = (AttrConfigProp<Boolean>) attrProps.getOrDefault(group.name(),
                            new AttrConfigProp<Boolean>());
                    usageProp.setSystemValue(metadata.isEnabledFor(group));
                    usageProp.setAllowCustomization(attrSpec == null || attrSpec.allowChange(group));
                    break;
                default:
                    continue;
                }
                mergeConfig.putProperty(group.name(), usageProp);
            }
            map.put(metadata.getAttrName(), mergeConfig);
        }
        return new ArrayList<>(map.values());
    }

    @SuppressWarnings("unchecked")
    private void modifyDeprecatedAttrState(AttrConfig attrConfig, ColumnMetadata metadata) {
        AttrConfigProp<AttrState> stateProp = (AttrConfigProp<AttrState>) attrConfig
                .getProperty(ColumnMetadataKey.State);
        AttrState systemVal = stateProp.getSystemValue();
        AttrState customVal = stateProp.getCustomValue();
        boolean deprecate = Boolean.TRUE.equals(metadata.getShouldDeprecate());
        AttrState finalVal = attrConfig.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
        if (deprecate) {
            // freeze the attribute in either case
            stateProp.setAllowCustomization(false);
            if (AttrState.Active.equals(finalVal)) {
                // should be deprecated but is active
                if (AttrState.Active.equals(customVal)) {
                    // activated by user -> this value will be changed back to
                    // Active when saved to database
                    customVal = AttrState.Deprecated;
                } else {
                    // activated by system -> this value won't be saved to
                    // database
                    systemVal = AttrState.Deprecated;
                }
            }
        }
        stateProp.setSystemValue(systemVal);
        stateProp.setCustomValue(customVal);
        attrConfig.putProperty(ColumnMetadataKey.State, stateProp);
    }

    @SuppressWarnings("unchecked")
    public List<AttrConfig> trim(List<AttrConfig> customConfig) {
        List<AttrConfig> results = new ArrayList<>();
        for (AttrConfig config : customConfig) {
            AttrState state = config.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
            if (AttrState.Active.equals(state)) {
                AttrConfigProp<AttrState> stateProp = (AttrConfigProp<AttrState>) config
                        .getProperty(ColumnMetadataKey.State);
                if (AttrState.Deprecated.equals(stateProp.getCustomValue())) {
                    stateProp.setCustomValue(AttrState.Active);
                }
                config.getAttrProps().values().removeIf(v -> (v.getCustomValue() == null));
                if (config.getAttrProps().size() != 0) {
                    config.getAttrProps().values().forEach(prop -> {
                        prop.setSystemValue(null);
                        prop.setAllowCustomization(null);
                    });
                    results.add(config);
                }
            }
        }
        return results;
    }
}