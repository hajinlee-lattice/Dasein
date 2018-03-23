package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.util.AttrTypeResolver;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

public abstract class AbstractAttrConfigService {

    @Inject
    private AttrConfigEntityMgr entityMgr;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected List<AttrConfig> getCustomConfig(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getTenantId();
        return entityMgr.findAllForEntity(tenantId, entity);
    }

    @SuppressWarnings("unchecked")
    public List<AttrConfig> render(List<ColumnMetadata> systemMetadata, List<AttrConfig> customConfig) {
        if (systemMetadata == null) {
            return customConfig;
        } else if (customConfig == null) {
            customConfig = new ArrayList<>();
        }
        Map<String, AttrConfig> map = new HashMap<String, AttrConfig>();
        for (AttrConfig config : customConfig) {
            map.put(config.getAttrName(), config);
        }
        for (ColumnMetadata metadata : systemMetadata) {
            AttrConfig mergeConfig = map.get(metadata.getAttrName());
            if (mergeConfig == null) {
                mergeConfig = new AttrConfig();
                mergeConfig.setAttrName(metadata.getAttrName());
                mergeConfig.setAttrType(AttrTypeResolver.resolveType(metadata));
                mergeConfig.setAttrProps(new HashMap<>());
            }
            Map<String, AttrConfigProp<?>> attrProps = mergeConfig.getAttrProps();
            if (attrProps == null) {
                attrProps = new HashMap<>();
            }

            AttrConfigProp<Category> cateProp = (AttrConfigProp<Category>) attrProps
                    .getOrDefault(ColumnMetadataKey.Category, new AttrConfigProp<Category>());
            cateProp.setSystemValue(metadata.getCategory());
            mergeConfig.putProperty(ColumnMetadataKey.Category, cateProp);

            AttrConfigProp<String> subCateProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Subcategory, new AttrConfigProp<String>());
            subCateProp.setSystemValue(metadata.getSubcategory());
            mergeConfig.putProperty(ColumnMetadataKey.Subcategory, subCateProp);

            AttrConfigProp<AttrState> statsProp = (AttrConfigProp<AttrState>) attrProps.getOrDefault(
                    ColumnMetadataKey.State,
                    new AttrConfigProp<AttrState>());
            AttrState state = AttrState.Deprecated;
            if (metadata.getAttrState() == null || AttrState.Active.equals(metadata.getAttrState())) {
                state = AttrState.Active;
            }
            statsProp.setSystemValue(state);
            mergeConfig.putProperty(ColumnMetadataKey.State, statsProp);

            AttrConfigProp<String> displayNameProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.DisplayName, new AttrConfigProp<String>());
            displayNameProp.setSystemValue(metadata.getDisplayName());
            mergeConfig.putProperty(ColumnMetadataKey.DisplayName, displayNameProp);

            AttrConfigProp<String> descriptionProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Description, new AttrConfigProp<String>());
            descriptionProp.setSystemValue(metadata.getDescription());
            mergeConfig.putProperty(ColumnMetadataKey.Description, descriptionProp);

            AttrConfigProp<Boolean> companyProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(AttrConfig.CompanyProfile, new AttrConfigProp<Boolean>());
            companyProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.CompanyProfile));
            mergeConfig.putProperty(AttrConfig.CompanyProfile, companyProp);

            AttrConfigProp<Boolean> enrichProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(AttrConfig.Enrichment, new AttrConfigProp<Boolean>());
            enrichProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Enrichment));
            mergeConfig.putProperty(AttrConfig.Enrichment, enrichProp);

            AttrConfigProp<Boolean> modelProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(AttrConfig.Model, new AttrConfigProp<Boolean>());
            modelProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Model));
            mergeConfig.putProperty(AttrConfig.Model, modelProp);

            AttrConfigProp<Boolean> segProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(AttrConfig.Segment, new AttrConfigProp<Boolean>());
            segProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Segment));
            mergeConfig.putProperty(AttrConfig.Segment, segProp);

            AttrConfigProp<Boolean> talkingPointProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(AttrConfig.TalkingPoint, new AttrConfigProp<Boolean>());
            talkingPointProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.TalkingPoint));
            mergeConfig.putProperty(AttrConfig.TalkingPoint, talkingPointProp);
            map.put(metadata.getAttrName(), mergeConfig);
        }
        return map.values().stream().collect(Collectors.toList());
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