package com.latticeengines.transform.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransformMetadata {

    private String name;
    private Map<String, String> properties = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return getPropertyValue("DisplayName");
    }

    public void setDisplayName(String displayName) {
        setPropertyValue("DisplayName", displayName);
    }

    public Integer getLength() {
        return Integer.valueOf(getPropertyValue("Length"));
    }

    public void setLength(Integer length) {
        setPropertyValue("Length", length.toString());
    }

    public Boolean isNullable() {
        return Boolean.valueOf(getPropertyValue("Nullable"));
    }

    public void setNullable(Boolean nullable) {
        setPropertyValue("Nullable", nullable.toString());
    }

    public Integer getPrecision() {
        return Integer.valueOf(getPropertyValue("Precision"));
    }

    public void setPrecision(Integer precision) {
        setPropertyValue("Precision", precision.toString());
    }

    public Integer getScale() {
        return Integer.valueOf(getPropertyValue("Scale"));
    }

    public void setScale(Integer scale) {
        setPropertyValue("Scale", scale.toString());
    }

    public List<String> getEnumValues() {
        return getListFromStringValues(getPropertyValue("EnumValues"));
    }

    public void setEnumValues(List<String> enumValues) {
        setPropertyValue("EnumValues", getStringValuesFromList(enumValues));
    }

    public String getPropertyValue(String key) {
        return properties.get(key);
    }

    public void setPropertyValue(String key, String value) {
        properties.put(key, value);
    }

    public Set<Map.Entry<String, String>> getEntries() {
        return properties.entrySet();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperties(Map<String, String> properties) {
        this.properties.putAll(properties);
    }

    public void setApprovedUsage(ApprovedUsage... approvedUsages) {
        setPropertyValue("ApprovedUsage", getStringValuesFromEnums(approvedUsages));
    }

    public void setApprovedUsage(List<String> approvedUsage) {
        setPropertyValue("ApprovedUsage", getStringValuesFromList(approvedUsage));
    }

    public List<String> getApprovedUsage() {
        return getListFromStringValues(getPropertyValue("ApprovedUsage"));
    }

    public void setStatisticalType(String statisticalType) {
        setPropertyValue("StatisticalType", statisticalType);
    }

    public void setStatisticalType(StatisticalType statisticalType) {
        setPropertyValue("StatisticalType", statisticalType.getName());
    }

    public String getStatisticalType() {
        return getPropertyValue("StatisticalType");
    }

    public void setFundamentalType(String fundamentalType) {
        setPropertyValue("FundamentalType", fundamentalType);
    }

    public void setFundamentalType(FundamentalType fundamentalType) {
        setPropertyValue("FundamentalType", fundamentalType.getName());
    }

    public String getFundamentalType() {
        return getPropertyValue("FundamentalType");
    }

    public void setDataQuality(String dataQuality) {
        setPropertyValue("DataQuality", dataQuality);
    }

    public String getDataQuality() {
        return getPropertyValue("DataQuality");
    }

    public void setDataSource(String dataSource) {
        setPropertyValue("DataSource", dataSource);
    }

    public void setDataSource(List<String> dataSource) {
        setPropertyValue("DataSource", getStringValuesFromList(dataSource));
    }

    public List<String> getDataSource() {
        return getListFromStringValues(getPropertyValue("DataSource"));
    }

    public void setDisplayDiscretizationStrategy(String displayDiscretizationStrategy) {
        setPropertyValue("DisplayDiscretizationStrategy", displayDiscretizationStrategy);
    }

    public String getDisplayDiscretizationStrategy() {
        return getPropertyValue("DisplayDiscretizationStrategy");
    }

    public void setDescription(String description) {
        setPropertyValue("Description", description);
    }

    public String getDescription() {
        return getPropertyValue("Description");
    }

    public void setTags(String tags) {
        setPropertyValue("Tags", tags);
    }

    public void setTags(Tag... tags) {
        setPropertyValue("Tags", getStringValuesFromEnums(tags));
    }

    public void setTags(List<String> tags) {
        setTags(getStringValuesFromList(tags));
    }

    public List<String> getTags() {
        return getListFromStringValues(getPropertyValue("Tags"));
    }

    public void setPhysicalName(String physicalName) {
        setPropertyValue("PhysicalName", physicalName);
    }

    public String getPhysicalName() {
        return getPropertyValue("PhysicalName");
    }

    public void setCategory(String category) {
        setPropertyValue("Category", category);
    }

    public void setCategory(Category category) {
        setPropertyValue("Category", category.getName());
    }

    public String getCategory() {
        return getPropertyValue("Category") != null ? getPropertyValue("Category").toString() : null;
    }

    public void setDataType(String dataType) {
        setPropertyValue("DataType", dataType);
    }

    public String getDataType() {
        return getPropertyValue("DataType") != null ? getPropertyValue("DataType").toString() : null;
    }

    public void setRTSModuleName(String rtsModuleName) {
        setPropertyValue("RTSModuleName", rtsModuleName);
    }

    public String getRTSModuleName() {
        return getPropertyValue("RTSModuleName");
    }

    public void setRTSArguments(String rtsArguments) {
        setPropertyValue("RTSArguments", rtsArguments);
    }

    public String getRTSArguments() {
        return getPropertyValue("RTSArguments");
    }

    public Boolean getRTSAttribute() {
        Boolean rts = Boolean.valueOf(getPropertyValue("RTSAttribute"));
        if (rts == null) {
            return false;
        }
        return rts;
    }

    public void setRTSAttribute(Boolean rts) {
        setPropertyValue("RTSAttribute", rts.toString());
    }

    public void setRTSAttribute(String rts) {
        setPropertyValue("RTSAttribute", rts);
    }

    public List<String> getAllowedDisplayNames() {
        return getListFromStringValues(getPropertyValue("AllowedDisplayNames"));
    }

    public void setAllowedDisplayNames(List<String> allowedDisplayNames) {
        setAllowedDisplayNames(getStringValuesFromList(allowedDisplayNames));
    }

    public void setAllowedDisplayNames(String allowedDisplayNamesString) {
        setPropertyValue("AllowedDisplayNames", allowedDisplayNamesString);
    }

    public String toString() {
        return name;
    }

    @SuppressWarnings({ "unchecked", "hiding" })
    public <Enum> String getStringValuesFromEnums(Enum... enums) {
        List<String> strs = new ArrayList<>();
        for (Enum en : enums) {
            strs.add(en.toString());
        }
        return strs.toString();
    }

    public String getStringValuesFromList(List<String> list) {
        StringBuilder builder = new StringBuilder();
        if (list.size() > 0) {
            builder.append(list.get(0));
        }
        for (int i = 1; i < list.size(); i++) {
            builder.append(",").append(list.get(i));
        }
        return builder.toString();
    }

    public List<String> getListFromStringValues(String input) {
        String[] split = input.split(",");
        return Arrays.<String> asList(split);
    }
}
