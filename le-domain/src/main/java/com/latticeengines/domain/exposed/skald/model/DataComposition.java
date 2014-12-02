package com.latticeengines.domain.exposed.skald.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Defines how to construct an input record for a PMML model.
public class DataComposition {
    public DataComposition() {
        this.fields = new HashMap<String, FieldSchema>();
        this.transforms = new ArrayList<TransformDefinition>();
    }

    // All the fields from the event table that was used to create this model.
    // This schema should match the input provided to the modeling service.
    // Keyed by internal name of the column associated with this field.
    public Map<String, FieldSchema> fields;

    // The set of transforms to create new fields based on the existing ones.
    // Theses will be executed in order, which should capture any dependencies.
    public List<TransformDefinition> transforms;
}
