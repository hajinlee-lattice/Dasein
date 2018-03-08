package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.IsColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttrConfig implements IsColumnMetadata {

    private static final long serialVersionUID = -118514979620559934L;

    @JsonProperty(ColumnMetadataKey.AttrName)
    private String attrName;

    @JsonProperty("Type")
    private AttrType attrType;

    @JsonProperty("Props")
    private Map<String, AttrConfigProp<?>> attrProps;

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

    public Map<String, AttrConfigProp<?>> getAttrProps() {
        return attrProps;
    }

    // Keys must be chosen from the constants in ColumnMetadataKey
    public void setAttrProps(Map<String, AttrConfigProp<?>> attrProps) {
        this.attrProps = attrProps;
    }

    public void putProperty(String key, AttrConfigProp attrProp) {
        if (attrProps == null) {
            attrProps = new HashMap<>();
        }
        attrProps.put(key, attrProp);
    }

    public AttrConfigProp getProperty(String key) {
        if (MapUtils.isNotEmpty(attrProps) && attrProps.containsKey(key)) {
            return attrProps.get(key);
        }
        return null;
    }

    private <T> T getProperty(String key, Class<T> valueClz) {
        AttrConfigProp prop = getProperty(key);
        if (prop != null && prop.getCustomValue() != null) {
            return valueClz.cast(prop.getCustomValue());
        }
        return null;
    }

    @Override
    public ColumnMetadata toColumnMetadata() {
        if (StringUtils.isBlank(getAttrName())) {
            throw new IllegalArgumentException("Must specify attribute name");
        }
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(getAttrName());
        cm.setDisplayName(getProperty(ColumnMetadataKey.DisplayName, String.class));
        for (ColumnSelection.Predefined group: ColumnSelection.Predefined.values()) {
            parseUsageGroup(cm, group);
        }
        cm.setDeprecated(AttrState.Deprecated.equals(getProperty("State", AttrState.class)));
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

}
