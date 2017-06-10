package com.latticeengines.dante.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({ "DefaultValue", "Interpretation", "InterpretationString", //
        "Nullable", "PropertyType", "PropertyTypeString", "Required", "TargetNotion", "PropertyAggregationString" })
public class PropertyMetadata extends BaseObjectMetadata {

}
