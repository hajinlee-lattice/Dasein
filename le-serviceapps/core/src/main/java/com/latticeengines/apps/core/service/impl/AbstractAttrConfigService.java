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
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

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
            AttrType type = AttrTypeResolver.resolveType(metadata);
            AttrSubType subType = AttrTypeResolver.resolveSubType(metadata);
            if (AttrType.Internal.equals(type)) {
                continue;
            }
            AttrConfig mergeConfig = map.get(metadata.getAttrName());
            if (mergeConfig == null) {
                mergeConfig = new AttrConfig();
                mergeConfig.setAttrName(metadata.getAttrName());
                mergeConfig.setAttrType(type);
                mergeConfig.setAttrSubType(subType);
                mergeConfig.setEntity(metadata.getEntity());
                mergeConfig.setAttrProps(new HashMap<>());
            }
            Map<String, AttrConfigProp<?>> attrProps = mergeConfig.getAttrProps();
            if (attrProps == null) {
                attrProps = new HashMap<>();
            }

            AttrConfigProp<Category> cateProp = (AttrConfigProp<Category>) attrProps
                    .getOrDefault(ColumnMetadataKey.Category, new AttrConfigProp<Category>());
            cateProp.setSystemValue(metadata.getCategory());
            cateProp.setAllowCustomization(resolveAllowCategOrSubCate(type, subType));
            mergeConfig.putProperty(ColumnMetadataKey.Category, cateProp);

            AttrConfigProp<String> subCateProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Subcategory, new AttrConfigProp<String>());
            subCateProp.setSystemValue(metadata.getSubcategory());
            subCateProp.setAllowCustomization(resolveAllowCategOrSubCate(type, subType));
            mergeConfig.putProperty(ColumnMetadataKey.Subcategory, subCateProp);

            AttrConfigProp<AttrState> statsProp = (AttrConfigProp<AttrState>) attrProps.getOrDefault(
                    ColumnMetadataKey.State,
                    new AttrConfigProp<AttrState>());
            AttrState state = AttrState.Deprecated;
            if (metadata.getAttrState() == null || AttrState.Active.equals(metadata.getAttrState())) {
                state = AttrState.Active;
            }
            statsProp.setSystemValue(state);
            statsProp.setAllowCustomization(resolveAllowStateOrDisplayName(type, subType));
            mergeConfig.putProperty(ColumnMetadataKey.State, statsProp);

            AttrConfigProp<String> displayNameProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.DisplayName, new AttrConfigProp<String>());
            displayNameProp.setSystemValue(metadata.getDisplayName());
            displayNameProp.setAllowCustomization(resolveAllowStateOrDisplayName(type, subType));
            mergeConfig.putProperty(ColumnMetadataKey.DisplayName, displayNameProp);

            AttrConfigProp<String> descriptionProp = (AttrConfigProp<String>) attrProps
                    .getOrDefault(ColumnMetadataKey.Description, new AttrConfigProp<String>());
            descriptionProp.setSystemValue(metadata.getDescription());
            descriptionProp.setAllowCustomization(resolveAllowDesc(type, subType));
            mergeConfig.putProperty(ColumnMetadataKey.Description, descriptionProp);

            AttrConfigProp<Boolean> companyProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(ColumnSelection.Predefined.CompanyProfile, new AttrConfigProp<Boolean>());
            companyProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.CompanyProfile));
            companyProp.setAllowCustomization(resolveAllowCompanyProfileOrEnrichment());
            mergeConfig.putProperty(ColumnSelection.Predefined.CompanyProfile.name(), companyProp);

            AttrConfigProp<Boolean> enrichProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(ColumnSelection.Predefined.Enrichment, new AttrConfigProp<Boolean>());
            enrichProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Enrichment));
            enrichProp.setAllowCustomization(resolveAllowCompanyProfileOrEnrichment());
            mergeConfig.putProperty(ColumnSelection.Predefined.Enrichment.name(), enrichProp);

            AttrConfigProp<Boolean> modelProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(ColumnSelection.Predefined.Model, new AttrConfigProp<Boolean>());
            modelProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Model));
            modelProp.setAllowCustomization(resolveAllowModel(type, subType));
            mergeConfig.putProperty(ColumnSelection.Predefined.Model.name(), modelProp);

            AttrConfigProp<Boolean> segProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(ColumnSelection.Predefined.Segment, new AttrConfigProp<Boolean>());
            segProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.Segment));
            segProp.setAllowCustomization(resolveAllowSegmentOrTalkingPoint(type, subType));
            mergeConfig.putProperty(ColumnSelection.Predefined.Segment.name(), segProp);

            AttrConfigProp<Boolean> talkingPointProp = (AttrConfigProp<Boolean>) attrProps
                    .getOrDefault(ColumnSelection.Predefined.TalkingPoint.name(), new AttrConfigProp<Boolean>());
            talkingPointProp.setSystemValue(metadata.isEnabledFor(ColumnSelection.Predefined.TalkingPoint));
            talkingPointProp.setAllowCustomization(resolveAllowSegmentOrTalkingPoint(type, subType));
            mergeConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), talkingPointProp);
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

    private boolean resolveAllowCategOrSubCate(AttrType type, AttrSubType subType) {
        return !((AttrType.Curated.equals(type) && AttrSubType.ProductBundle.equals(subType))
                || (AttrType.Custom.equals(type) && AttrSubType.Standard.equals(subType)));
    }

    private boolean resolveAllowDesc(AttrType type, AttrSubType subType) {
        return (AttrType.Custom.equals(type) && !AttrSubType.Standard.equals(subType))
                || (AttrType.Curated.equals(type) && !AttrSubType.Rating.equals(subType));
    }

    private boolean resolveAllowStateOrDisplayName(AttrType type, AttrSubType subType) {
        return false;
    }

    private boolean resolveAllowCompanyProfileOrEnrichment() {
        return true;
    }

    private boolean resolveAllowModel(AttrType type, AttrSubType subType) {
        return AttrType.DataCloud.equals(type)
                || (AttrType.Custom.equals(type) && !AttrSubType.LookupId.equals(subType));
    }

    private boolean resolveAllowSegmentOrTalkingPoint(AttrType type, AttrSubType subType) {
        return !(AttrType.DataCloud.equals(type) && AttrSubType.InternalEnrich.equals(subType));
    }
}