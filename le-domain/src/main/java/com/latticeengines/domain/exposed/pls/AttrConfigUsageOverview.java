package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigUsageOverview {
    public static final long defaultExportLimit = 1000L;
    public static final long defaultCompanyProfileLimit = 200L;

    @JsonProperty("AttrNums")
    private Map<String, Long> attrNums;

    @JsonProperty("Selections")
    private List<AttrConfigSelection> selections;

    public void setAttrNums(Map<String, Long> attrNums) {
        this.attrNums = attrNums;
    }

    public Map<String, Long> getAttrNums() {
        return this.attrNums;
    }

    public void setSelections(List<AttrConfigSelection> selections) {
        this.selections = selections;
    }

    public List<AttrConfigSelection> getSelections() {
        return this.selections;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
