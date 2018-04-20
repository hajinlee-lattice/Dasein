package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.util.AttrTypeResolver;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.common.exposed.timer.PerformanceTimer;

public abstract class AbstractAttrConfigService implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(AbstractAttrConfigService.class);

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private AttrValidationService attrValidationService;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected abstract List<ColumnMetadata> getSystemMetadata(Category category);

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render) {
        String tenantId = MultiTenantContext.getTenantId();
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
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
    public List<AttrConfig> getRenderedList(Category category) {
        List<AttrConfig> renderedList;
        String tenantId = MultiTenantContext.getTenantId();
        BusinessEntity entity = CategoryUtils.getEntity(category);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(category);
            renderedList = render(columns, customConfig);
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

        String tenantId = MultiTenantContext.getTenantId();
        mergeConfigWithExisting(tenantId, attrConfigGrps);

        List<AttrConfig> renderedList;
        Map<BusinessEntity, List<AttrConfig>> attrConfigGrpsForTrim = new HashMap<>();
        if (MapUtils.isEmpty(attrConfigGrps)) {
            toReturn = request;
        } else {
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
                    Callable<List<AttrConfig>> callable =
                            () -> {
                                MultiTenantContext.setTenant(tenant);
                                List<AttrConfig> renderedConfigList = renderForEntity(configList, entity);
                                attrConfigGrpsForTrim.put(entity, renderedConfigList);
                                return renderedConfigList;
                            };
                    callables.add(callable);
                });

                // fork join execution
                ExecutorService threadPool = ThreadPoolUtils.getForkJoinThreadPool("attr-config", 4);
                List<List<AttrConfig>> lists = ThreadPoolUtils.runOnThreadPool(threadPool, callables);
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
                throw new IllegalArgumentException(
                        "Request has validation errors, cannot be saved: " + JsonUtils.serialize(validated.getDetails()));
            }

            log.info("AttrConfig before saving is " + JsonUtils.serialize(request));
            // trim and save
            attrConfigGrpsForTrim.forEach((entity, configList) -> {
                attrConfigEntityMgr
                        .save(MultiTenantContext.getTenantId(), entity, trim(configList));
            });
        }

        return toReturn;
    }

    /**
     * Merge with existing configs in Document DB
     */
    private void mergeConfigWithExisting(String tenantId, Map<BusinessEntity, List<AttrConfig>> attrConfigGrps) {
        attrConfigGrps.forEach((entity, configList) -> {
            List<AttrConfig> existingConfigs = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            Map<String, AttrConfig> existingMap = new HashMap<>();
            existingConfigs.forEach(config -> existingMap.put(config.getAttrName(), config));
            for (AttrConfig config : configList) {
                AttrConfig existingConfig = existingMap.get(config.getAttrName());
                if (existingConfig != null) {
                    existingConfig.getAttrProps().forEach((name, value) -> {
                        if (!config.getAttrProps().containsKey(name)) {
                            config.getAttrProps().put(name, value);
                        }
                    });
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
                log.warn(String.format("Cannot get Attr Specification for Type %s, SubType %s",
                        type.name(), subType.name()));
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

            Map<String, AttrConfigProp<?>> attrProps = mergeConfig.getAttrProps();
            if (attrProps == null) {
                attrProps = new HashMap<>();
            }

            AttrConfigProp<Category> cateProp = (AttrConfigProp<Category>) attrProps
                    .getOrDefault(ColumnMetadataKey.Category, new AttrConfigProp<Category>());
            cateProp.setSystemValue(metadata.getCategory());
            cateProp.setAllowCustomization(attrSpec == null ? true : attrSpec.categoryNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.Category, cateProp);

            AttrConfigProp<String> subCateProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Subcategory, new AttrConfigProp<String>());
            subCateProp.setSystemValue(metadata.getSubcategory());
            subCateProp.setAllowCustomization(attrSpec == null ? true : attrSpec.categoryNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.Subcategory, subCateProp);

            AttrConfigProp<AttrState> statsProp = (AttrConfigProp<AttrState>) attrProps
                    .getOrDefault(ColumnMetadataKey.State, new AttrConfigProp<AttrState>());
            AttrState state;
            if (metadata.getAttrState() == null) {
                state = AttrState.Active;
            } else {
                state = metadata.getAttrState();
            }
            statsProp.setSystemValue(state);
            statsProp.setAllowCustomization(true);
            mergeConfig.putProperty(ColumnMetadataKey.State, statsProp);

            AttrConfigProp<String> displayNameProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.DisplayName, new AttrConfigProp<String>());
            displayNameProp.setSystemValue(metadata.getDisplayName());
            displayNameProp.setAllowCustomization(attrSpec == null ? true : attrSpec.displayNameChange());
            mergeConfig.putProperty(ColumnMetadataKey.DisplayName, displayNameProp);

            AttrConfigProp<String> descriptionProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Description, new AttrConfigProp<String>());
            descriptionProp.setSystemValue(metadata.getDescription());
            descriptionProp.setAllowCustomization(attrSpec == null ? true : attrSpec.descriptionChange());
            mergeConfig.putProperty(ColumnMetadataKey.Description, descriptionProp);

            for (ColumnSelection.Predefined group: ColumnSelection.Predefined.values()) {
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
                        usageProp.setAllowCustomization(attrSpec == null ? true : attrSpec.allowChange(group));
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

    public List<AttrConfig> trim(List<AttrConfig> customConfig) {
        List<AttrConfig> results = new ArrayList<AttrConfig>();
        for (AttrConfig config : customConfig) {
            Map<String, AttrConfigProp<?>> props = config.getAttrProps();
            AttrState state = (AttrState) (props.get(ColumnMetadataKey.State)).getSystemValue();
            if (state != null && state.equals(AttrState.Active)) {
                config.getAttrProps().values().removeIf(v -> (v.getCustomValue() == null));
                if (config.getAttrProps().size() != 0) {
                    results.add(config);
                }
            }
        }
        return results;
    }
}