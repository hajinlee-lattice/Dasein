package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DimensionalQuery {

    @JsonProperty("Source")
    private String source;

    @JsonProperty("Dimension")
    private String dimension;

    @JsonProperty("Qualifiers")
    private Map<String, String> qualifiers;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public Map<String, String> getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(Map<String, String> qualifiers) {
        this.qualifiers = qualifiers;
    }

    @Override
    public String toString() {
        String prefix = source + "." + dimension + ".";
        List<String> segments = new ArrayList<>();
        if (qualifiers != null) {
            for (Map.Entry<String, String> entry : qualifiers.entrySet()) {
                segments.add(prefix + entry.getKey() + "=" + entry.getValue());
            }
            return StringUtils.join(segments, "&");
        } else {
            return prefix + "null";
        }
    }

}
