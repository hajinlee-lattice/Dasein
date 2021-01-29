package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Metadata for {@link StreamDimension} (possible values and other info)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionMetadata implements Serializable {
    public static final String DIMENSION_VALUES_KEY = "dimension_values";
    public static final String CARDINALITY_KEY = "cardinality";

    private static final long serialVersionUID = -2500038370595664739L;

    @JsonProperty(DIMENSION_VALUES_KEY)
    private List<Map<String, Object>> dimensionValues = new ArrayList<>();

    @JsonProperty(CARDINALITY_KEY)
    private long cardinality;

    public List<Map<String, Object>> getDimensionValues() {
        return dimensionValues;
    }

    public void setDimensionValues(List<Map<String, Object>> dimensionValues) {
        this.dimensionValues = dimensionValues;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DimensionMetadata metadata = (DimensionMetadata) o;
        return cardinality == metadata.cardinality && Objects.equal(dimensionValues, metadata.dimensionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dimensionValues, cardinality);
    }
}
