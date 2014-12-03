package com.latticeengines.domain.exposed.skald.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Defines how to construct an input record for a PMML model.
public class DataComposition {
    public DataComposition(Map<String, FieldSchema> fields, List<TransformDefinition> transforms) {
        this.fields = fields;
        this.transforms = transforms;
    }
    
    // Serialization constructor.
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((transforms == null) ? 0 : transforms.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataComposition other = (DataComposition) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (transforms == null) {
            if (other.transforms != null)
                return false;
        } else if (!transforms.equals(other.transforms))
            return false;
        return true;
    }
}
