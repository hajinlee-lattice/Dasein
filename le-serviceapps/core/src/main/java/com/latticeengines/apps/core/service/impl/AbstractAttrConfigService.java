package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import com.latticeengines.baton.exposed.service.BatonService;
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
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.AttributeSetResponse;
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
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.proxy.exposed.cdl.CDLDanteConfigProxy;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public abstract class AbstractAttrConfigService implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AbstractAttrConfigService.class);

    private static final long DEFAULT_LIMIT = 500L;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private AttrValidationService attrValidationService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private CDLDanteConfigProxy cdlDanteConfigProxy;

    @Inject
    protected BatonService batonService;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected abstract List<ColumnMetadata> getSystemMetadata(Category category);

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render) {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId, entity);
            if (render) {
                List<ColumnMetadata> columns = getSystemMetadata(entity);
                renderedList = render(columns, customConfig, entityMatchEnabled);
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
    public List<AttrConfig> getRenderedList(String propertyName, Boolean enabled) {
        log.info("propertyNames are " + propertyName + ", enabled " + enabled);
        List<Category> categories = Arrays.stream(Category.values()).filter(category -> !category.isHiddenFromUi())
                .collect(Collectors.toList());
        final Tenant tenant = MultiTenantContext.getTenant();
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());

        List<AttrConfig> configs = Collections.synchronizedList(new ArrayList<>());
        List<Runnable> runnables = new ArrayList<>();
        categories.forEach(category -> {
            Runnable runnable = () -> {
                MultiTenantContext.setTenant(tenant);
                List<AttrConfig> attrConfigs = getRenderedList(category, entityMatchEnabled).stream()
                        .filter(config -> enabled.equals(config.getPropertyFinalValue(propertyName, Boolean.class)))
                        .collect(Collectors.toList());
                configs.addAll(attrConfigs);
            };
            runnables.add(runnable);
        });
        // fork join execution
        ThreadPoolUtils.runInParallel("attr-config", runnables);
        return configs;
    }

    @Override
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean activeOnly) {
        return getAttrConfigOverview(categories, propertyNames, activeOnly, null);
    }

    @Override
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean activeOnly, String attributeName) {
        ConcurrentMap<String, AttrConfigCategoryOverview<?>> attrConfigOverview = new ConcurrentHashMap<>();
        log.info("categories are" + categories + ", propertyNames are " + propertyNames + ", activeOnly " + activeOnly);
        final Tenant tenant = MultiTenantContext.getTenant();
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());

        List<Runnable> runnables = new ArrayList<>();
        categories.forEach(category -> {
            Runnable runnable = () -> {
                MultiTenantContext.setTenant(tenant);
                AttrConfigCategoryOverview<?> attrConfigCategoryOverview = getAttrConfigOverview(
                        getRenderedList(category, entityMatchEnabled, attributeName), category, propertyNames,
                        activeOnly);
                attrConfigOverview.put(category.getName(), attrConfigCategoryOverview);
            };
            runnables.add(runnable);
        });
        // fork join execution
        ThreadPoolUtils.runInParallel("attr-config", runnables);
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
            case DNB_TECHNOLOGY_PROFILE:
                long dmx = DEFAULT_LIMIT;
                try {
                    dmx = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.DMX.getDataLicense());
                } catch (Exception e) {
                    log.warn("Failed to get max premium lead enrichment attrs from ZK", e);
                }
                overview.setLimit(dmx);
                break;
            case WEBSITE_KEYWORDS:
                // TODO going to get rid of the try catch after the zookeeper is
                // updated for all tenants
                long defaultWebsiteKeywords = 200L;
                try {
                    defaultWebsiteKeywords = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.WEBSITEKEYWORDS.getDataLicense());
                } catch (Exception e) {
                    log.warn("Error getting the limit for website keyword " + MultiTenantContext.getTenant().getId());
                }
                overview.setLimit(defaultWebsiteKeywords);
                break;
            case ACCOUNT_ATTRIBUTES:
                // TODO going to get rid of the try catch after the zookeeper is
                // updated for all tenants
                long accounts = DEFAULT_LIMIT;
                try {
                    accounts = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.ACCOUNT.getDataLicense());
                } catch (Exception e) {
                    log.warn("Failed to get max premium lead enrichment attrs from ZK", e);
                }
                overview.setLimit(accounts);
                break;
            case CONTACT_ATTRIBUTES:
                long contacts = DEFAULT_LIMIT;
                try {
                    contacts = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.CONTACT.getDataLicense());
                } catch (Exception e) {
                    log.warn("Failed to get max premium lead enrichment attrs from ZK", e);
                }
                overview.setLimit(contacts);
                break;
            case GROWTH_TRENDS:
                long trends = DEFAULT_LIMIT;
                try {
                    trends = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.GROWTHTRENDS.getDataLicense());
                } catch (Exception e) {
                    log.warn("Failed to get max premium lead enrichment attrs from ZK", e);
                }
                overview.setLimit(trends);
                break;
            case COVID_19:
                long covid19s = DEFAULT_LIMIT;
                try {
                    covid19s = zkConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(
                            MultiTenantContext.getShortTenantId(), DataLicense.COVID19.getDataLicense());
                } catch (Exception e) {
                    log.warn("Failed to get max premium lead enrichment attrs from ZK", e);
                }
                overview.setLimit(covid19s);
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
                     * DP-6630 For Activate/Deactivate page, hide attributes that are: Inactive and
                     * AllowCustomization=FALSE
                     *
                     * For Enable/Disable page, hide attributes that are: disabled and
                     * AllowCustomization=FALSE.
                     *
                     * PLS-11145 For Enable/Disable page, hide attributes that are: disabled and
                     * Deprecated
                     *
                     * 'onlyActivateAttrs=false' indicates it is Activate/Deactivate page, otherwise
                     * it is Usage Enable/Disable page
                     */
                    boolean includeCurrentAttr = true;
                    AttrConfigProp<AttrState> attrConfigProp = (AttrConfigProp<AttrState>) attrProps
                            .get(ColumnMetadataKey.State);
                    if (attrConfigProp == null) {
                        log.warn("Attr config with null state prop : " + JsonUtils.serialize(attrConfig));
                        continue;
                    }
                    if (attrConfigProp.isAllowCustomization() == null) {
                        log.info("Attr config allowCustomiztion is null " + JsonUtils.serialize(attrConfig));
                    }
                    if (onlyActiveAttrs) {
                        // PLS-10731 activation status does not apply to
                        // Modeling usage
                        if (AttrState.Inactive.equals(getActualValue(attrConfigProp))
                                && !ColumnSelection.Predefined.Model.name().equals(propertyName)) {
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
                             * For Enable/Disable page, hide attributes that are: disabled and
                             * AllowCustomization=FALSE.
                             *
                             * For Enable/Disable page, hide attributes that are: disabled and Deprecated
                             */
                            totalAttrs++;
                            if (onlyActiveAttrs) {
                                if (Boolean.FALSE.equals(actualValue) && (!configProp.isAllowCustomization()
                                        || Boolean.TRUE.equals(attrConfig.getShouldDeprecate()))) {
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
            // the total Attr count only makes sense for activation overview
            if (!onlyActiveAttrs) {
                overview.setTotalAttrs(totalAttrs);
            }
        }
        return overview;
    }

    @VisibleForTesting
    <T extends Serializable> Object getActualValue(AttrConfigProp<T> configProp) {
        if (Boolean.TRUE.equals(configProp.isAllowCustomization()) && configProp.getCustomValue() != null) {
            return configProp.getCustomValue();
        }
        return configProp.getSystemValue();
    }

    @Override
    public List<AttrConfig> getRenderedList(Category category) {
        return getRenderedList(category, null);
    }

    @Override
    public List<AttrConfig> getRenderedList(Category category, String attributeSetName) {
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
        return getRenderedList(category, entityMatchEnabled, attributeSetName);
    }

    private List<AttrConfig> getRenderedList(Category category, boolean entityMatchEnabled) {
        return getRenderedList(category, entityMatchEnabled, null);
    }

    private List<AttrConfig> getRenderedList(Category category, boolean entityMatchEnabled, String attributeSetName) {
        List<AttrConfig> renderedList;
        String tenantId = MultiTenantContext.getShortTenantId();
        List<BusinessEntity> entities = CategoryUtils.getEntity(category);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllInEntitiesInReader(tenantId, entities);
            List<ColumnMetadata> columns = getSystemMetadata(category);
            Set<String> columnsInSystem = columns.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
            List<AttrConfig> customConfigInCategory = customConfig.stream() //
                    .filter(attrConfig -> category
                            .equals(attrConfig.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class))
                            || columnsInSystem.contains(attrConfig.getAttrName()))
                    .collect(Collectors.toList());
            Set<String> attributesInSet = null;
            if (!AttributeUtils.isDefaultAttributeSet(attributeSetName)) {
                AttributeSet attributeSet = getAttributeSetByName(attributeSetName);
                if (attributeSet != null) {
                    Map<String, Set<String>> attributeMap = attributeSet.getAttributesMap();
                    if (MapUtils.isNotEmpty(attributeMap)) {
                        attributesInSet = attributeMap.getOrDefault(category.name(), new HashSet<>());
                    } else {
                        attributesInSet = new HashSet<>();
                    }
                }
            }
            renderedList = render(columns, customConfigInCategory, entityMatchEnabled, attributesInSet);
            modifyInactivateState(renderedList);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs for entities %s", count,
                    Arrays.toString(entities.toArray()));
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public AttrConfigRequest validateRequest(AttrConfigRequest request, AttrConfigUpdateMode mode) {
        return validateRequest(request, mode, null);
    }

    @Override
    public AttrConfigRequest validateRequest(AttrConfigRequest request, AttrConfigUpdateMode mode,
            Map<BusinessEntity, Set<String>> entitySetMap) {
        String tenantId = MultiTenantContext.getShortTenantId();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
            Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = renderConfigs(request.getAttrConfigs(),
                    new ArrayList<>(), entityMatchEnabled, entitySetMap == null);
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
            BusinessEntity.SEGMENT_ENTITIES.forEach(entity -> {
                Runnable runnable = () -> {
                    MultiTenantContext.setTenant(tenant);
                    List<ColumnMetadata> systemMetadataCols = getSystemMetadata(entity);
                    List<AttrConfig> existingCustomConfig = attrConfigEntityMgr.findAllForEntityInReader(tenantId,
                            entity);
                    existingAttrConfigs.addAll(render(systemMetadataCols, existingCustomConfig, entityMatchEnabled,
                            getAttributesInEntity(entitySetMap, entity)));
                };
                runnables.add(runnable);
            });
            ThreadPoolUtils.runInParallel("attr-config", runnables);
            log.info("user provided configs in validation request is " + JsonUtils.serialize(request.getAttrConfigs()));
            ValidationDetails details = attrValidationService.validate(existingAttrConfigs, request.getAttrConfigs(),
                    mode);
            request.setDetails(details);
            int count = CollectionUtils.isNotEmpty(request.getAttrConfigs()) ? request.getAttrConfigs().size() : 0;
            String msg = String.format("Validate %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return request;
    }

    private Set<String> getAttributesInEntity(Map<BusinessEntity, Set<String>> entitySetMap, BusinessEntity entity) {
        if (entitySetMap != null) {
            Set<String> attributes = entitySetMap.get(entity);
            if (attributes == null) {
                attributes = new HashSet<>();
            }
            return attributes;
        } else {
            return null;
        }
    }

    @Override
    public AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode) {
        return saveRequest(request, mode, false);
    }

    private void setExistingAttrConfigs(Tracer tracer, String tenantId, AttrConfigUpdateMode mode,
            boolean entityMatchEnabled, List<AttrConfig> existingAttrConfigs) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Span renderSpan = tracer //
                    .buildSpan("renderAttrConfig") //
                    .withTag(TracingTags.TENANT_ID, tenantId) //
                    .withTag(TracingTags.Attribute.ATTR_CONFIG_UPDATE_MODE, mode == null ? "" : mode.name()) //
                    .start();
            try (Scope renderScope = tracer.activateSpan(renderSpan)) {
                final Tenant tenant = MultiTenantContext.getTenant();
                List<Runnable> runnables = new ArrayList<>();
                BusinessEntity.SEGMENT_ENTITIES.forEach(entity -> {
                    Runnable runnable = () -> {
                        Span renderEntitySpan = tracer.buildSpan("renderAttrConfigForEntity") //
                                .withTag(TracingTags.ENTITY, entity.name()) //
                                .asChildOf(renderSpan) //
                                .start();
                        try (Scope scope = tracer.activateSpan(renderEntitySpan)) {
                            MultiTenantContext.setTenant(tenant);
                            List<ColumnMetadata> systemMetadataCols = getSystemMetadataCols(entity, tracer);
                            List<AttrConfig> existingCustomConfig = attrConfigEntityMgr
                                    .findAllForEntityInReader(tenantId, entity);
                            renderEntitySpan.log(String.format("start rendering with # of system metadata cols = %d",
                                    CollectionUtils.size(systemMetadataCols)));
                            List<AttrConfig> configs = render(systemMetadataCols, existingCustomConfig,
                                    entityMatchEnabled);
                            renderEntitySpan
                                    .log(String.format("# of attr configs is %d", CollectionUtils.size(configs)));
                            existingAttrConfigs.addAll(configs);
                        } finally {
                            TracingUtils.finish(renderEntitySpan);
                        }
                    };
                    runnables.add(runnable);
                });
                ThreadPoolUtils.runInParallel("attr-config", runnables);
                int count = CollectionUtils.isNotEmpty(existingAttrConfigs) ? existingAttrConfigs.size() : 0;
                String msg = String.format("Rendered %d attr configs for tenant %s", count, tenantId);
                renderSpan.log(msg);
                timer.setTimerMessage(msg);
            } finally {
                TracingUtils.finish(renderSpan);
            }
        }
    }

    @Override
    public AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode,
            boolean updateDefaultSet) {
        AttrConfigRequest toReturn;
        String tenantId = MultiTenantContext.getShortTenantId();
        List<AttrConfig> attrConfigs = request.getAttrConfigs();
        List<AttrConfigEntity> toDeleteEntities = new ArrayList<>();
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = renderConfigs(attrConfigs, toDeleteEntities,
                entityMatchEnabled, true);
        if (MapUtils.isEmpty(attrConfigGrpsForTrim)) {
            toReturn = request;
        } else {
            List<AttrConfig> userProvidedList = generateListFromMap(attrConfigGrpsForTrim);
            log.info("user provided configs" + JsonUtils.serialize(userProvidedList));
            toReturn = new AttrConfigRequest();
            toReturn.setAttrConfigs(userProvidedList);
            // Render the system metadata decorated by existing custom data
            List<AttrConfig> existingAttrConfigs = Collections.synchronizedList(new ArrayList<>());
            Tracer tracer = GlobalTracer.get();
            setExistingAttrConfigs(tracer, tenantId, mode, entityMatchEnabled, existingAttrConfigs);
            ValidationDetails details = attrValidationService.validate(existingAttrConfigs, userProvidedList, mode);
            toReturn.setDetails(details);
            TracingUtils.logInActiveSpan(Collections.singletonMap("validationDetails", JsonUtils.serialize(details)));
            if (toReturn.hasWarning() || toReturn.hasError()) {
                log.warn("current attribute configs has warnings or errors:" + JsonUtils.serialize(details));
                TracingUtils.logError(tracer.activeSpan(), null,
                        "attr config validation result has warnings or errors");
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
                saveAttrConfigs(shortTenantId, entity, configList);
                // clear serving metadata cache
                String keyPrefix = shortTenantId + "|" + entity.name();
                cacheService.refreshKeysByPattern(keyPrefix, CacheName.getCdlServingCacheGroup());
            });
            cdlDanteConfigProxy.refreshDanteConfiguration(tenantId);
            cacheService.refreshKeysByPattern(MultiTenantContext.getShortTenantId(), CacheName.DataLakeStatsCubesCache);
        }
        if (updateDefaultSet) {
            updateDefaultSet();
        }
        return toReturn;
    }

    private void updateDefaultSet() {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setName(AttributeUtils.DEFAULT_ATTRIBUTE_SET_NAME);
        updateAttributeSet(attributeSet);
    }

    private List<ColumnMetadata> getSystemMetadataCols(BusinessEntity entity, Tracer tracer) {
        Span getMetadataSpan = tracer //
                .buildSpan("getSystemMetadataCols") //
                .withTag(TracingTags.ENTITY, entity == null ? "" : entity.name()) //
                .start();
        try {
            return getSystemMetadata(entity);
        } catch (Exception e) {
            TracingUtils.logError(getMetadataSpan, e,
                    String.format("Failed to get metadata columns for entity %s", entity));
            throw e;
        } finally {
            TracingUtils.finish(getMetadataSpan);
        }
    }

    private void saveAttrConfigs(String shortTenantId, BusinessEntity entity, List<AttrConfig> configList) {
        Span saveSpan = GlobalTracer.get().buildSpan("saveAttrConfigList").start();
        try {
            attrConfigEntityMgr.save(shortTenantId, entity, trim(configList));
            saveSpan.log(String.format("Save attr config list (size=%d)", CollectionUtils.size(configList)));
        } catch (Exception e) {
            TracingUtils.logError(saveSpan, e, "Failed to save attr configs");
        } finally {
            TracingUtils.finish(saveSpan);
        }
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
            List<AttrConfigEntity> toDeleteEntities, boolean entityMatchEnabled, boolean mergeUsageGroupProps) {
        // split by entity
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrps = new HashMap<>();
        if (CollectionUtils.isEmpty(attrConfigs)) {
            return attrConfigGrps;
        }
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
            mergeConfigWithExisting(tenantId, attrConfigGrps, toDeleteEntities, mergeUsageGroupProps);
            ConcurrentMap<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = new ConcurrentHashMap<>();

            if (attrConfigGrps.size() == 1) {
                BusinessEntity entity = new ArrayList<>(attrConfigGrps.keySet()).get(0);
                List<AttrConfig> renderedList = renderForEntity(attrConfigGrps.get(entity), entity, entityMatchEnabled);
                attrConfigGrpsForTrim.put(entity, renderedList);
            } else {
                log.info("Saving attr configs for " + attrConfigGrps.size() + " entities in parallel.");
                // distribute to tasklets
                final Tenant tenant = MultiTenantContext.getTenant();
                List<Runnable> runnables = new ArrayList<>();
                attrConfigGrps.forEach((entity, configList) -> {
                    Runnable runnable = () -> {
                        MultiTenantContext.setTenant(tenant);
                        List<AttrConfig> renderedConfigList = renderForEntity(configList, entity, entityMatchEnabled);
                        attrConfigGrpsForTrim.put(entity, renderedConfigList);
                    };
                    runnables.add(runnable);
                });

                // fork join execution
                ThreadPoolUtils.runInParallel("attr-config", runnables);
            }
            return attrConfigGrpsForTrim;
        }

    }

    /**
     * Merge with existing configs in Document DB
     */
    private void mergeConfigWithExisting(String tenantId, Map<BusinessEntity, List<AttrConfig>> attrConfigGrps,
            List<AttrConfigEntity> toDeleteEntities, boolean mergeUsageGroupProps) {

        Map<BusinessEntity, List<AttrConfigEntity>> entityMap = new HashMap<>();
        List<AttrConfigEntity> allConfigEntities = attrConfigEntityMgr.findAllObjectsInEntitiesInReader(tenantId,
                new ArrayList<>(attrConfigGrps.keySet()));
        allConfigEntities.forEach(entry -> {
            if (entry.getDocument() != null) {
                BusinessEntity entity = entry.getDocument().getEntity();
                entityMap.putIfAbsent(entity, new ArrayList());
                entityMap.get(entity).add(entry);
            }

        });
        attrConfigGrps.forEach((entity, configList) -> {
            List<AttrConfigEntity> existingConfigEntities = entityMap.getOrDefault(entity, new ArrayList<>());

            Map<String, AttrConfig> existingMap = new HashMap<>();
            Map<String, AttrConfigEntity> existingEntityMap = new HashMap<>();
            existingConfigEntities.forEach(configEntity -> {
                AttrConfig config = configEntity.getDocument();
                if (config != null) {
                    existingMap.put(config.getAttrName(), config);
                    existingEntityMap.put(config.getAttrName(), configEntity);
                }
            });
            Set<String> groupNames = Arrays.stream(ColumnSelection.Predefined.values()).map(Enum::name)
                    .collect(Collectors.toSet());
            for (AttrConfig config : configList) {
                String attrName = config.getAttrName();
                AttrConfig existingConfig = existingMap.get(attrName);
                if (existingConfig != null) {
                    // write user changed prop
                    existingConfig.getAttrProps().forEach((propName, propValue) -> {
                        if (!config.getAttrProps().containsKey(propName)) {
                            // need to merge other properties for attribute set because attribute set
                            // already has its
                            // own group info
                            if (mergeUsageGroupProps || !groupNames.contains(propName)) {
                                config.getAttrProps().put(propName, propValue);
                            }
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
    private List<AttrConfig> renderForEntity(List<AttrConfig> configList, BusinessEntity entity,
            boolean entityMatchEnabled) {
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Set<String> attrNames = configList.stream().map(AttrConfig::getAttrName).collect(Collectors.toSet());
            List<ColumnMetadata> systemMds = getSystemMetadata(entity);
            List<ColumnMetadata> columns = systemMds.stream() //
                    .filter(cm -> attrNames.contains(cm.getAttrName())) //
                    .collect(Collectors.toList());
            renderedList = render(columns, configList, entityMatchEnabled);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs in entity %s for saving", count, entity);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public Map<BusinessEntity, List<AttrConfig>> findAllHaveCustomDisplayNameByTenantId(String tenantId) {
        Map<BusinessEntity, List<AttrConfig>> result = new HashMap<>();
        List<AttrConfig> totalList = attrConfigEntityMgr.findAllHaveCustomDisplayNameByTenantId(tenantId);
        totalList.forEach(attrConfig -> {
            if (result.containsKey(attrConfig.getEntity())) {
                result.get(attrConfig.getEntity()).add(attrConfig);
            } else {
                List<AttrConfig> list = new ArrayList<>();
                list.add(attrConfig);
                result.put(attrConfig.getEntity(), list);
            }
        });
        return result;
    }

    @Override
    public void removeAttrConfig(String tenantId) {
        attrConfigEntityMgr.cleanupTenant(tenantId);
    }

    @Override
    public void removeAttrConfigForEntity(String tenantId, BusinessEntity entity) {
        attrConfigEntityMgr.deleteAllForEntity(tenantId, entity);
    }

    public List<AttrConfig> render(List<ColumnMetadata> systemMetadata, List<AttrConfig> customConfigs,
            boolean entityMatchEnabled) {
        return render(systemMetadata, customConfigs, entityMatchEnabled, null);
    }

    @SuppressWarnings("unchecked")
    private void setStringProperty(AttrConfig mergeConfig, String key, String value, Boolean allowCustomization,
            Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<String> prop = (AttrConfigProp<String>) attrProps.getOrDefault(key,
                new AttrConfigProp<String>());
        prop.setSystemValue(value);
        prop.setAllowCustomization(allowCustomization);
        mergeConfig.putProperty(key, prop);
    }

    @SuppressWarnings("unchecked")
    private void setCategoryProperty(AttrConfig mergeConfig, Category value, Boolean allowCustomization,
            Map<String, AttrConfigProp<?>> attrProps) {
        AttrConfigProp<Category> cateProp = (AttrConfigProp<Category>) attrProps
                .getOrDefault(ColumnMetadataKey.Category, new AttrConfigProp<Category>());
        cateProp.setSystemValue(value);
        cateProp.setAllowCustomization(allowCustomization);
        mergeConfig.putProperty(ColumnMetadataKey.Category, cateProp);
    }

    @SuppressWarnings("unchecked")
    private void setStateProperty(AttrConfig mergeConfig, ColumnMetadata metadata, AttrSpecification attrSpec,
            Map<String, AttrConfigProp<?>> attrProps) {
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
    }

    private AttrConfig getAttrConfig(AttrType type, AttrSubType subType, ColumnMetadata metadata,
            Map<String, AttrConfig> customConfigMap, Set<String> renderedAttrNames, Set<String> attributesInSet) {
        AttrSpecification attrSpec = AttrSpecification.getAttrSpecification(type, subType, metadata.getEntity());
        if (attrSpec == null) {
            log.warn(String.format("Cannot get Attr Specification for Type %s, SubType %s", type.name(),
                    subType.name()));
        }
        AttrConfig mergeConfig = customConfigMap.get(metadata.getAttrName());
        if (mergeConfig == null) {
            mergeConfig = new AttrConfig();
            mergeConfig.setAttrName(metadata.getAttrName());
            mergeConfig.setAttrProps(new HashMap<>());
        } else {
            renderedAttrNames.remove(mergeConfig.getAttrName());
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
        setCategoryProperty(mergeConfig, metadata.getCategory(), attrSpec == null || attrSpec.categoryNameChange(),
                attrProps);
        setStringProperty(mergeConfig, ColumnMetadataKey.Subcategory, metadata.getSubcategory(),
                attrSpec == null || attrSpec.categoryNameChange(), attrProps);
        setStateProperty(mergeConfig, metadata, attrSpec, attrProps);
        modifyDeprecatedAttrState(mergeConfig, metadata);
        setStringProperty(mergeConfig, ColumnMetadataKey.DisplayName, metadata.getDisplayName(),
                attrSpec == null || attrSpec.displayNameChange(), attrProps);
        setStringProperty(mergeConfig, ColumnMetadataKey.Description, metadata.getDescription(),
                attrSpec == null || attrSpec.descriptionChange(), attrProps);
        setStringProperty(mergeConfig, ColumnMetadataKey.ApprovedUsage, metadata.getApprovedUsageString(),
                attrSpec == null || attrSpec.approvedUsageChange(), attrProps);
        setUsageGroup(mergeConfig, attrProps, metadata, attrSpec, attributesInSet);
        return mergeConfig;
    }

    public List<AttrConfig> render(List<ColumnMetadata> systemMetadata, List<AttrConfig> customConfigs,
            boolean entityMatchEnabled, Set<String> attributesInSet) {
        if (systemMetadata == null) {
            throw new LedpException(LedpCode.LEDP_40022);
        } else if (customConfigs == null) {
            customConfigs = new ArrayList<>();
        }
        Map<String, AttrConfig> customConfigMap = new HashMap<>();
        Map<String, AttrConfig> renderedMap = new HashMap<>();
        for (AttrConfig config : customConfigs) {
            customConfigMap.put(config.getAttrName(), config);
        }
        Set<String> renderedAttrNames = customConfigs.stream().map(AttrConfig::getAttrName).collect(Collectors.toSet());
        for (ColumnMetadata metadata : systemMetadata) {
            AttrType type = AttrTypeResolver.resolveType(metadata, entityMatchEnabled);
            AttrSubType subType = AttrTypeResolver.resolveSubType(metadata, entityMatchEnabled);
            if (AttrType.Internal.equals(type)) {
                if (renderedAttrNames.contains(metadata.getAttrName())) {
                    renderedAttrNames.remove(metadata.getAttrName());
                }
                continue;
            }
            AttrConfig mergeConfig = getAttrConfig(type, subType, metadata, customConfigMap, renderedAttrNames,
                    attributesInSet);
            renderedMap.put(metadata.getAttrName(), mergeConfig);
        }
        // make sure the system metadata include the customer config
        if (CollectionUtils.isNotEmpty(renderedAttrNames)) {
            log.warn(String.format("Wrong customer config, system can't render these attributes %s for tenant %s",
                    renderedAttrNames.toString(), MultiTenantContext.getCustomerSpace()));
        }
        return new ArrayList<>(renderedMap.values());
    }

    @SuppressWarnings("unchecked")
    private void setUsageGroup(AttrConfig mergeConfig, Map<String, AttrConfigProp<?>> attrProps,
            ColumnMetadata metadata, AttrSpecification attrSpec, Set<String> attributesInSet) {
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
                if (usageProp.isAllowCustomization() && attributesInSet != null
                        && ColumnSelection.Predefined.Enrichment.equals(group)) {
                    if (attributesInSet.contains(metadata.getAttrName())) {
                        usageProp.setCustomValue(Boolean.TRUE);
                    } else {
                        usageProp.setCustomValue(Boolean.FALSE);
                    }
                }
                break;
            default:
                continue;
            }
            mergeConfig.putProperty(group.name(), usageProp);
        }
    }

    private void overwriteAttrSpecsByColMetadata(AttrSpecification attrSpec, ColumnMetadata cm) {
        if (attrSpec != null) {
            // do not overwrite anything if can flag is empty
            if (Boolean.FALSE.equals(cm.getCanEnrich())) {
                attrSpec.setEnrichmentChange(false);
                attrSpec.setTalkingPointChange(false);
                attrSpec.setCompanyProfileChange(false);
            }
            if (Boolean.FALSE.equals(cm.getCanSegment())) {
                attrSpec.setSegmentationChange(false);
            }
            if (Boolean.FALSE.equals(cm.getCanEnrich()) //
                    && Boolean.FALSE.equals(cm.getCanSegment()) //
                    && Boolean.FALSE.equals(cm.getCanModel())) {
                attrSpec.disableStateChange();
            }
            if (Boolean.FALSE.equals(cm.getCanModel())) {
                attrSpec.setModelChange(false);
            }
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
                    // PLS-13636 Modeling usage customization not
                    // impacted by State
                    if (!key.equals(ColumnMetadataKey.State) && !key.equals(ColumnSelection.Predefined.Model.name()))
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
            attrConfig.setShouldDeprecate(true);
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
            config.setShouldDeprecate(null);
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

    @Override
    public AttributeSet getAttributeSetByName(String name) {
        return null;
    }

    @Override
    public List<AttributeSet> getAttributeSets(boolean withAttributesMap) {
        return null;
    }

    @Override
    public AttributeSet cloneAttributeSet(String attributeSetName, AttributeSet attributeSet) {
        return null;
    }

    @Override
    public AttributeSetResponse createAttributeSet(AttributeSet attributeSet) {
        return null;
    }

    @Override
    public AttributeSetResponse updateAttributeSet(AttributeSet attributeSet) {
        return null;
    }

    @Override
    public void deleteAttributeSetByName(String name) {

    }
}
