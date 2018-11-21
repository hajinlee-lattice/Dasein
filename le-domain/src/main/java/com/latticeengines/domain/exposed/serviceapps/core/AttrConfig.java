package com.latticeengines.domain.exposed.serviceapps.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.IsColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttrConfig implements IsColumnMetadata, Cloneable {

    private static final long serialVersionUID = -118514979620559934L;

    @JsonProperty(ColumnMetadataKey.AttrName)
    private String attrName;

    @JsonIgnore
    private AttrType attrType;

    @JsonIgnore
    private AttrSubType attrSubType;

    @JsonProperty("Entity")
    private BusinessEntity entity;

    @JsonProperty("DataLicense")
    private String dataLicense;

    @JsonProperty("ShouldDeprecate")
    private Boolean shouldDeprecate = Boolean.FALSE;

    @JsonProperty("Props")
    private Map<String, AttrConfigProp<?>> attrProps;

    @JsonProperty("Impact_Warning")
    private ImpactWarnings impactWarnings;

    @JsonProperty("Validation_Error")
    private ValidationErrors validationErrors;

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public AttrType getAttrType() {
        return attrType;
    }

    public void setAttrType(AttrType attrType) {
        this.attrType = attrType;
    }

    public Boolean getShouldDeprecate() {
        return this.shouldDeprecate;
    }

    public void setShouldDeprecate(Boolean shouldDeprecate) {
        this.shouldDeprecate = shouldDeprecate;
    }

    public AttrSubType getAttrSubType() {
        return attrSubType;
    }

    public void setAttrSubType(AttrSubType attrSubType) {
        this.attrSubType = attrSubType;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public Map<String, AttrConfigProp<?>> getAttrProps() {
        return attrProps;
    }

    // Keys must be chosen from the constants in ColumnMetadataKey
    public void setAttrProps(Map<String, AttrConfigProp<?>> attrProps) {
        this.attrProps = attrProps;
    }

    public void putProperty(String key, AttrConfigProp<?> attrProp) {
        if (attrProps == null) {
            attrProps = new HashMap<>();
        }
        attrProps.put(key, attrProp);
    }

    public AttrConfigProp<?> getProperty(String key) {
        if (MapUtils.isNotEmpty(attrProps) && attrProps.containsKey(key)) {
            return attrProps.get(key);
        }
        return null;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> AttrConfigProp<T> getStrongTypedProperty(String key, Class<T> valClz) {
        if (MapUtils.isNotEmpty(attrProps) && attrProps.containsKey(key)) {
            return (AttrConfigProp<T>) attrProps.get(key);
        }
        return null;
    }

    public <T> T getPropertyFinalValue(String key, Class<T> valueClz) {
        AttrConfigProp<?> prop = getProperty(key);
        if (prop != null) {
            if (Boolean.TRUE.equals(prop.isAllowCustomization()) && prop.getCustomValue() != null) {
                return valueClz.cast(prop.getCustomValue());
            } else {
                return valueClz.cast(prop.getSystemValue());
            }
        }
        return null;
    }

    private <T> T getProperty(String key, Class<T> valueClz) {
        AttrConfigProp<?> prop = getProperty(key);
        if (prop != null && prop.getCustomValue() != null) {
            return valueClz.cast(prop.getCustomValue());
        }
        return null;
    }

    public ImpactWarnings getImpactWarnings() {
        return impactWarnings;
    }

    public void setImpactWarnings(ImpactWarnings impactWarnings) {
        this.impactWarnings = impactWarnings;
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(ValidationErrors validationErrors) {
        this.validationErrors = validationErrors;
    }

    public String getDataLicense() {
        return dataLicense;
    }

    public void setDataLicense(String dataLicense) {
        this.dataLicense = dataLicense;
    }

    @Override
    public ColumnMetadata toColumnMetadata() {
        if (StringUtils.isBlank(getAttrName())) {
            throw new IllegalArgumentException("Must specify attribute name");
        }
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(getAttrName());
        cm.setDisplayName(getProperty(ColumnMetadataKey.DisplayName, String.class));
        cm.setDescription(getProperty(ColumnMetadataKey.Description, String.class));
        for (ColumnSelection.Predefined group : ColumnSelection.Predefined.values()) {
            parseUsageGroup(cm, group);
        }
        cm.setAttrState(getProperty(ColumnMetadataKey.State, AttrState.class));
        return cm;
    }

    private void parseUsageGroup(ColumnMetadata cm, ColumnSelection.Predefined group) {
        Boolean config = getProperty(group.name(), Boolean.class);
        if (config != null) {
            if (config) {
                cm.enableGroup(group);
            } else {
                cm.disableGroup(group);
            }
        }
    }

    public void fixJsonDeserialization() {
        if (attrProps.containsKey(ColumnMetadataKey.State)) {
            AttrConfigProp<?> prop = attrProps.get(ColumnMetadataKey.State);
            AttrConfigProp<AttrState> typeSafeProp = new AttrConfigProp<>();
            typeSafeProp.setAllowCustomization(prop.isAllowCustomization());
            if (prop.getCustomValue() != null) {
                Object val = prop.getCustomValue();
                AttrState state = (val instanceof String) ? AttrState.valueOf((String) val) : (AttrState) val;
                typeSafeProp.setCustomValue(state);
            }
            if (prop.getSystemValue() != null) {
                Object val = prop.getSystemValue();
                AttrState state = (val instanceof String) ? AttrState.valueOf((String) val) : (AttrState) val;
                typeSafeProp.setSystemValue(state);
            }

            attrProps.put(ColumnMetadataKey.State, typeSafeProp);
        }

        if (attrProps.containsKey(ColumnMetadataKey.Category)) {
            AttrConfigProp<?> prop = attrProps.get(ColumnMetadataKey.Category);
            AttrConfigProp<Category> typeSafeProp = new AttrConfigProp<>();
            typeSafeProp.setAllowCustomization(prop.isAllowCustomization());
            if (prop.getCustomValue() != null) {
                Object val = prop.getCustomValue();
                Category category = (val instanceof String) ? Category.valueOf((String) val) : (Category) val;
                typeSafeProp.setCustomValue(category);
            }
            if (prop.getSystemValue() != null) {
                Object val = prop.getSystemValue();
                Category category = (val instanceof String) ? Category.valueOf((String) val) : (Category) val;
                typeSafeProp.setSystemValue(category);
            }

            attrProps.put(ColumnMetadataKey.Category, typeSafeProp);
        }

    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof AttrConfig)) {
            return false;
        }
        AttrConfig config = (AttrConfig) o;
        boolean flag1 = StringUtils.equals(attrName, config.getAttrName()) && attrType == config.getAttrType()
                && attrSubType == config.getAttrSubType() && entity == config.getEntity();
        if (!flag1) {
            return false;
        }
        boolean flag2 = true;
        Map<String, AttrConfigProp<?>> attrProps2 = config.getAttrProps();
        if (attrProps == attrProps2) {
            flag2 = true;
        } else if (attrProps != null && attrProps2 != null) {
            if (attrProps.size() != attrProps2.size()) {
                flag2 = false;
            } else {
                for (Map.Entry<String, AttrConfigProp<?>> entry : attrProps.entrySet()) {
                    String key = entry.getKey();
                    AttrConfigProp<?> val = entry.getValue();
                    if (!(val == attrProps2.get(key) || (val != null && val.equals(attrProps2.get(key))))) {
                        flag2 = false;
                        break;
                    }
                }
            }
        } else {
            flag2 = false;
        }
        return flag2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AttrConfig clone() {
        AttrConfig obj = null;
        try {
            obj = (AttrConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        HashMap<String, AttrConfigProp<?>> map = null;
        if (this.attrProps instanceof HashMap) {
            map = (HashMap<String, AttrConfigProp<?>>) this.attrProps;
            obj.attrProps = (Map<String, AttrConfigProp<?>>) map.clone();
        }
        return obj;
    }
}
