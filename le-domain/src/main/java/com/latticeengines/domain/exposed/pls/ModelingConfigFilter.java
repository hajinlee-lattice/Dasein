package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.ComparisonType;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents ModelingConfigFilter JSON Object")
public class ModelingConfigFilter {

    @JsonProperty("configName")
    private CrossSellModelingConfigKeys configName;

    @JsonProperty("criteria")
    private ComparisonType criteria;

    @JsonProperty("value")
    private Integer value;

    public ModelingConfigFilter() {

    }

    public ModelingConfigFilter(CrossSellModelingConfigKeys configName, ComparisonType criteria, Integer value) {
        this.configName = configName;
        this.criteria = criteria;
        this.value = value;
    }

    public CrossSellModelingConfigKeys getConfigName() {
        return configName;
    }

    public void setConfigName(CrossSellModelingConfigKeys configName) {
        this.configName = configName;
    }

    public ComparisonType getCriteria() {
        return criteria;
    }

    public void setCriteria(ComparisonType criteria) {
        this.criteria = criteria;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ModelingConfigFilter)) {
            return false;
        }
        ModelingConfigFilter newObj = (ModelingConfigFilter) obj;
        return this.configName != null && this.configName.equals(newObj.configName);
    }

    @Override
    public int hashCode() {
        return this.configName != null ? this.configName.hashCode() : super.hashCode();
    }

}
