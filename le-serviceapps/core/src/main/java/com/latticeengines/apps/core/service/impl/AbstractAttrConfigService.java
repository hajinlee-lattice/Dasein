package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.core.util.AttrTypeResolver;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
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
    private ActivationLimitValidator limitationValidator;

    @Inject
    private ZKConfigService zkConfigService;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected abstract List<ColumnMetadata> getSystemMetadata(Category category);

    public static final long DEFAULT_LIMIT = 500L;
    private static ExecutorService workers;

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render) {
        String tenantId = MultiTenantContext.getShortTenantId();
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
        ThreadPoolUtils.runRunnablesInParallel(getWorkers(), runnables, 10, 1);
        return attrConfigOverview;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    <T extends Serializable> AttrConfigCategoryOverview<T> getAttrConfigOverview(@NonNull List<AttrConfig> renderedList,
            Category category, List<String> propertyNames, boolean onlyActiveAttrs) {
        AttrConfigCategoryOverview<T> overview = new AttrConfigCategoryOverview<>();
        Map<String, Map<T, Long>> propSummary = new HashMap<>();
        overview.setPropSummary(propSummary);
        if (category.isPremium()) {
            switch (category) {
            case INTENT:
                overview.setLimit((long) zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getShortTenantId(), DataLicense.BOMBORA.getDataLicense()));
                break;
            case TECHNOLOGY_PROFILE:
                overview.setLimit((long) zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                        MultiTenantContext.getShortTenantId(), DataLicense.HG.getDataLicense()));
                break;
            case WEBSITE_KEYWORDS:
                // TODO going to get rid of the try catch after the zookeeper is
                // updated for all tenants
                long defaultWebsiteKeywords = 200L;
                try {
                    defaultWebsiteKeywords = (long) zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.WEBSITEKEYWORDS.getDataLicense());
                } catch (Exception e) {
                    log.warn("Error getting the limit for website keyword " + MultiTenantContext.getTenant().getId());
                }
                overview.setLimit(defaultWebsiteKeywords);
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

        log.info("Trying to get detailed config for " + category + " with properties: " + propertyNames);
        for (String propertyName : propertyNames) {
            Map<T, Long> valueNumberMap = new HashMap<>();
            propSummary.put(propertyName, valueNumberMap);
            long totalAttrs = 0;
            for (AttrConfig attrConfig : renderedList) {
                Map<String, AttrConfigProp<?>> attrProps = attrConfig.getAttrProps();
                if (attrProps != null) {
                    /*
                     * DP-6630 For Activate/Deactivate page, hide attributes
                     * that are: Inactive and AllowCustomization=FALSE
                     * 
                     * For Enable/Disable page, hide attributes that are:
                     * disabled and AllowCustomization=FALSE.
                     * 
                     * 'onlyActivateAttrs=false' indicates it is
                     * Activate/Deactivate page
                     */
                    boolean includeCurrentAttr = true;
                    AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                            .get(ColumnMetadataKey.State);
                    if (onlyActiveAttrs) {
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))) {
                            includeCurrentAttr = false;
                        }
                    } else {
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))
                                && !attrConfigProp.isAllowCustomization()) {
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

                            /*
                             * For Enable/Disable page, hide attributes that
                             * are: disabled and AllowCustomization=FALSE.
                             */
                            totalAttrs++;
                            if (onlyActiveAttrs) {
                                if (!configProp.isAllowCustomization() && Boolean.FALSE.equals(actualValue)) {
                                    continue;
                                }
                            }
                            Long count = valueNumberMap.getOrDefault(actualValue, 0L);
                            valueNumberMap.put((T) actualValue, count + 1);
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
        String tenantId = MultiTenantContext.getShortTenantId();
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
            modifyInactivateState(renderedList);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Deprecated
    @Override
    public AttrConfigRequest validateRequest(AttrConfigRequest request, AttrConfigUpdateMode mode) {
        String tenantId = MultiTenantContext.getShortTenantId();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = renderConfigs(request.getAttrConfigs(),
                    new ArrayList<>());
            if (MapUtils.isNotEmpty(attrConfigGrpsForTrim)) {
                List<AttrConfig> renderedList = attrConfigGrpsForTrim.values().stream().flatMap(list -> {
                    if (CollectionUtils.isNotEmpty(list)) {
                        return list.stream();
                    } else {
                        return Stream.empty();
                    }
                }).collect(Collectors.toList());
                request.setAttrConfigs(renderedList);
                // Render the system metadata decorated by existing custom data
                List<AttrConfig> existingAttrConfigs = Collections.synchronizedList(new ArrayList<>());

                final Tenant tenant = MultiTenantContext.getTenant();
                List<Runnable> runnables = new ArrayList<>();
                BusinessEntity.SEGMENT_ENTITIES.stream().forEach(entity -> {
                    Runnable runnable = () -> {
                        MultiTenantContext.setTenant(tenant);
                        List<ColumnMetadata> systemMetadataCols = getSystemMetadata(entity);
                        List<AttrConfig> existingCustomConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId,
                                entity);
                        existingAttrConfigs.addAll(render(systemMetadataCols, existingCustomConfig));
                    };
                    runnables.add(runnable);
                });
                ThreadPoolUtils.runRunnablesInParallel(getWorkers(), runnables, 10, 1);

                ValidationDetails details = attrValidationService.validate(existingAttrConfigs,
                        request.getAttrConfigs(), mode);
                request.setDetails(details);
            }
            int count = CollectionUtils.isNotEmpty(request.getAttrConfigs()) ? request.getAttrConfigs().size() : 0;
            String msg = String.format("Validate %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return request;
    }

    @Override
    public AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode) {
        AttrConfigRequest toReturn;
        String tenantId = MultiTenantContext.getShortTenantId();
        List<AttrConfig> attrConfigs = request.getAttrConfigs();
        List<AttrConfigEntity> toDeleteEntities = new ArrayList<>();
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = renderConfigs(attrConfigs, toDeleteEntities);
        if (MapUtils.isEmpty(attrConfigGrpsForTrim)) {
            toReturn = request;
        } else {
            List<AttrConfig> userProvidedList = generateListFromMap(attrConfigGrpsForTrim);
            log.info("user provided configs" + JsonUtils.serialize(userProvidedList));
            toReturn = new AttrConfigRequest();
            toReturn.setAttrConfigs(userProvidedList);

            // Render the system metadata decorated by existing custom data
            List<AttrConfig> existingAttrConfigs = Collections.synchronizedList(new ArrayList<>());

            try (PerformanceTimer timer = new PerformanceTimer()) {
                final Tenant tenant = MultiTenantContext.getTenant();
                List<Runnable> runnables = new ArrayList<>();
                BusinessEntity.SEGMENT_ENTITIES.stream().forEach(entity -> {
                    Runnable runnable = () -> {
                        MultiTenantContext.setTenant(tenant);
                        List<ColumnMetadata> systemMetadataCols = getSystemMetadata(entity);
                        List<AttrConfig> existingCustomConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId,
                                entity);
                        existingAttrConfigs.addAll(render(systemMetadataCols, existingCustomConfig));
                    };
                    runnables.add(runnable);
                });
                ThreadPoolUtils.runRunnablesInParallel(getWorkers(), runnables, 10, 1);
                int count = CollectionUtils.isNotEmpty(existingAttrConfigs) ? existingAttrConfigs.size() : 0;
                String msg = String.format("Rendered %d attr configs for tenant %s", count, tenantId);
                timer.setTimerMessage(msg);
            }

            ValidationDetails details = attrValidationService.validate(existingAttrConfigs, userProvidedList, mode);
            toReturn.setDetails(details);
            if (toReturn.hasWarning() || toReturn.hasError()) {
                log.warn("current attribute configs has warnings or errors:" + JsonUtils.serialize(details));
                return toReturn;
            }

            // after validation, delete the entities with empty props
            if (CollectionUtils.isNotEmpty(toDeleteEntities)) {
                attrConfigEntityMgr.deleteConfigs(toDeleteEntities);
            }
            log.info("AttrConfig before saving is " + JsonUtils.serialize(toReturn));
            CacheService cacheService = CacheServiceBase.getCacheService();
            // trim and save
            attrConfigGrpsForTrim.forEach((entity, configList) -> {
                String shortTenantId = MultiTenantContext.getShortTenantId();
                attrConfigEntityMgr.save(shortTenantId, entity, trim(configList));
                // clear serving metadata cache
                String key = shortTenantId + "|" + entity.name() + "|decoratedmetadata";
                cacheService.refreshKeysByPattern(key, CacheName.ServingMetadataCache);
            });
            cacheService.refreshKeysByPattern(MultiTenantContext.getShortTenantId(), CacheName.DataLakeStatsCubesCache);
        }

        return toReturn;
    }

    private List<AttrConfig> generateListFromMap(Map<BusinessEntity, List<AttrConfig>> map) {
        return map.values().stream().flatMap(list -> {
            if (CollectionUtils.isNotEmpty(list)) {
                return list.stream();
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toList());
    }

    /*
     * split configs by entity, then distribute thread to render separately
     */
    private Map<BusinessEntity, List<AttrConfig>> renderConfigs(List<AttrConfig> attrConfigs,
            List<AttrConfigEntity> toDeleteEntities) {
        // split by entity
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrps = new HashMap<>();
        attrConfigs.forEach(attrConfig -> {
            BusinessEntity entity = attrConfig.getEntity();
            if (!attrConfigGrps.containsKey(entity)) {
                attrConfigGrps.put(entity, new ArrayList<>());
            }
            attrConfigGrps.get(entity).add(attrConfig);
        });

        if (MapUtils.isEmpty(attrConfigGrps)) {
            return attrConfigGrps;
        } else {
            String tenantId = MultiTenantContext.getShortTenantId();
            mergeConfigWithExisting(tenantId, attrConfigGrps, toDeleteEntities);

            ConcurrentMap<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = new ConcurrentHashMap<>();

            if (attrConfigGrps.size() == 1) {
                BusinessEntity entity = new ArrayList<>(attrConfigGrps.keySet()).get(0);
                List<AttrConfig> renderedList = renderForEntity(attrConfigGrps.get(entity), entity);
                attrConfigGrpsForTrim.put(entity, renderedList);
            } else {
                log.info("Saving attr configs for " + attrConfigGrps.size() + " entities in parallel.");
                // distribute to tasklets
                final Tenant tenant = MultiTenantContext.getTenant();
                List<Runnable> runnables = new ArrayList<>();
                attrConfigGrps.forEach((entity, configList) -> {
                    Runnable runnable = () -> {
                        MultiTenantContext.setTenant(tenant);
                        List<AttrConfig> renderedConfigList = renderForEntity(configList, entity);
                        attrConfigGrpsForTrim.put(entity, renderedConfigList);
                    };
                    runnables.add(runnable);
                });

                // fork join execution
                ThreadPoolUtils.runRunnablesInParallel(getWorkers(), runnables, 10, 1);
            }
            return attrConfigGrpsForTrim;
        }

    }

    /**
     * Merge with existing configs in Document DB
     */
    private void mergeConfigWithExisting(String tenantId, Map<BusinessEntity, List<AttrConfig>> attrConfigGrps,
            List<AttrConfigEntity> toDeleteEntities) {

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
                    // write user changed prop
                    existingConfig.getAttrProps().forEach((propName, propValue) -> {
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
    public List<AttrConfig> render(List<ColumnMetadata> systemMetadata, List<AttrConfig> customConfigs) {
        if (systemMetadata == null) {
            throw new LedpException(LedpCode.LEDP_40022);
        } else if (customConfigs == null) {
            customConfigs = new ArrayList<>();
        }
        Map<String, AttrConfig> map = new HashMap<>();
        for (AttrConfig config : customConfigs) {
            map.put(config.getAttrName(), config);
        }
        List<String> renderedAttrNames = customConfigs.stream().map(config -> config.getAttrName())
                .collect(Collectors.toList());
        ThreadLocal<List<String>> nameslocal = ThreadLocal.withInitial(() -> renderedAttrNames);
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
            } else {
                nameslocal.get().remove(mergeConfig.getAttrName());
            }
            mergeConfig.setAttrType(type);
            mergeConfig.setAttrSubType(subType);
            mergeConfig.setEntity(metadata.getEntity());
            mergeConfig.setDataLicense(metadata.getDataLicense());
            overwriteAttrSpecsByColMetadata(attrSpec, metadata);

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
            stateProp.setAllowCustomization(attrSpec == null || attrSpec.stateChange());
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

            AttrConfigProp<String> approvedUsageProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.ApprovedUsage, new AttrConfigProp<String>());
            approvedUsageProp.setSystemValue(metadata.getApprovedUsageString());
            approvedUsageProp.setAllowCustomization(attrSpec == null || attrSpec.approvedUsageChange());
            mergeConfig.putProperty(ColumnMetadataKey.ApprovedUsage, approvedUsageProp);

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
        // make sure the system metadata include the customer config
        if (CollectionUtils.isNotEmpty(nameslocal.get())) {
            throw new LedpException(LedpCode.LEDP_40023, new String[] { nameslocal.get().toString() });
        }
        return new ArrayList<>(map.values());
    }

    private void overwriteAttrSpecsByColMetadata(AttrSpecification attrSpec, ColumnMetadata cm) {
        if (!Boolean.TRUE.equals(cm.getCanEnrich())) {
            attrSpec.setEnrichmentChange(false);
            attrSpec.setTalkingPointChange(false);
            attrSpec.setCompanyProfileChange(false);
        }
        if (!Boolean.TRUE.equals(cm.getCanSegment())) {
            attrSpec.setSegmentationChange(false);
        }
        if (!Boolean.TRUE.equals(cm.getCanEnrich()) //
                && !Boolean.TRUE.equals(cm.getCanSegment()) //
                && !Boolean.TRUE.equals(cm.getCanModel())) {
            attrSpec.disableStateChange();
        }
        if (!Boolean.TRUE.equals(cm.getCanModel())) {
            attrSpec.setModelChange(false);
        }
    }

    private void modifyInactivateState(List<AttrConfig> renderedConfigs) {
        // set allow customization to false when final value of state prop is
        // inactive
        // in other words, active and deprecated states will not change allow
        // customization
        renderedConfigs.forEach(attrConfig -> {
            AttrState state = attrConfig.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
            if (AttrState.Inactive.equals(state)) {
                attrConfig.getAttrProps().forEach((key, value) -> {
                    if (!key.equals(ColumnMetadataKey.State))
                        value.setAllowCustomization(false);
                });
            }
        });
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
            if (AttrState.Active.equals(finalVal)) {
                // allow customer to deactivate it
                stateProp.setAllowCustomization(true);
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
            } else {
                // cannot activate
                stateProp.setAllowCustomization(false);
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
            config.setImpactWarnings(null);
            config.setValidationErrors(null);

            AttrConfigProp<AttrState> stateProp = (AttrConfigProp<AttrState>) config
                    .getProperty(ColumnMetadataKey.State);
            if (AttrState.Deprecated.equals(stateProp.getCustomValue())) {
                stateProp.setCustomValue(AttrState.Active);
            }
            // save only the minimum information into database
            config.getAttrProps().values().removeIf(v -> (v.getCustomValue() == null));
            if (config.getAttrProps().size() != 0) {
                config.getAttrProps().values().forEach(prop -> {
                    prop.setSystemValue(null);
                    prop.setAllowCustomization(null);
                });
                results.add(config);
            }

        }
        return results;
    }

    private static ExecutorService getWorkers() {
        if (workers == null) {
            synchronized (AbstractAttrConfigService.class) {
                if (workers == null) {
                    workers = ThreadPoolUtils.getCachedThreadPool("attr-config-svc");
                }
            }
        }
        return workers;
    }
}