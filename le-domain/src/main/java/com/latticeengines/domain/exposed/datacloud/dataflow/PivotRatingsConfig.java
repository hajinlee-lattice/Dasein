package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PivotRatingsConfig extends TransformerConfig {

    @JsonProperty("id_attrs_map")
    private Map<String, String> idAttrsMap; // model id to engine id mapping

    @JsonProperty("ev_model_ids")
    private List<String> evModelIds;

    @JsonProperty("lift_chart_model_ids")
    private List<String> liftChartModelIds;

    public Map<String, String> getIdAttrsMap() {
        return idAttrsMap;
    }

    public void setIdAttrsMap(Map<String, String> idAttrsMap) {
        this.idAttrsMap = idAttrsMap;
    }

    public List<String> getEvModelIds() {
        return evModelIds;
    }

    public void setEvModelIds(List<String> evModelIds) {
        this.evModelIds = evModelIds;
    }

    public List<String> getLiftChartModelIds() {
        return liftChartModelIds;
    }

    public void setLiftChartModelIds(List<String> liftChartModelIds) {
        this.liftChartModelIds = liftChartModelIds;
    }
}
